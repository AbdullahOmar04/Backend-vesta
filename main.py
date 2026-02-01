from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # ADD THIS
import requests
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
import uvicorn

# --- Firebase Setup ---
if not firebase_admin._apps:
    # Try to load credentials from JSON string (for Render/production)
    firebase_cred_json = os.getenv("FIREBASE_CREDENTIALS")

    if firebase_cred_json:
        # Parse JSON string from environment variable
        cred_dict = json.loads(firebase_cred_json)
        cred = credentials.Certificate(cred_dict)
    else:
        # Fallback to file path (for local development)
        firebase_cred_path = os.getenv("FIREBASE_CRED_PATH", "serviceAccountKey.json")
        cred = credentials.Certificate(firebase_cred_path)

    firebase_admin.initialize_app(cred)

db = firestore.client()


# --- FastAPI App ---
app = FastAPI(
    title="Vesta Backend",
    description="MVP backend for syncing accounts and transactions with Firebase",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# --- CORS Configuration --- ADD THIS ENTIRE SECTION
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://vesta-7e96a.web.app",
        "https://vesta-7e96a.firebaseapp.com",
        "https://vestaapp.co",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "✅ Vesta backend is live and running 🚀"}

ACC_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Accounts/v0.4.3"
TRANS_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Transactions/v0.4.3/accounts"
SOSP_BASE_URL = "https://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Standing%20Orders%20&%20Scheduled%20Payments%20(SOSPs)/v0.4.3"


# --- Sync Accounts Endpoint ---
@app.get("/sync_accounts/{uid}/{customer_id}")
def sync_accounts(uid: str, customer_id: str):
    url = f"{ACC_BASE_URL}/accounts"
    headers = {"x-customer-id": customer_id}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        accounts = response.json().get("data", []) or []

        user_ref = db.collection("users").document(uid)
        accounts_ref = user_ref.collection("accounts")

        # Delete existing *unlinked* accounts only (keep already-linked ones if you want)
        # If you want to wipe everything every sync, keep your original delete loop.
        for doc in accounts_ref.stream():
            data = doc.to_dict() or {}
            if data.get("linked") != True:  # only delete non-linked cache
                doc.reference.delete()

        batch = db.batch()
        out = []

        for acc in accounts:
            account_id = str(acc.get("accountId", "")).strip()
            if not account_id:
                continue

            # Parse balance
            bal_raw = acc.get("availableBalance", {}).get("balanceAmount", 0)
            try:
                balance = float(bal_raw)
            except Exception:
                balance = 0.0

            currency = (acc.get("accountCurrency") or "JOD").strip()

            account_type_code = (acc.get("accountType", {}) or {}).get("code", "") or ""
            account_type_name = (acc.get("accountType", {}) or {}).get("name", "") or ""

            bank_name = (
                (acc.get("institutionBasicInfo", {}) or {})
                .get("name", {}) or {}
            ).get("enName") or "Unknown Bank"

            iban = (acc.get("mainRoute", {}) or {}).get("address") or ""

            trimmed = {
                "accountId": account_id,
                "provider": "JoPACC",
                "linked": False,  # <-- IMPORTANT

                "bankName": bank_name,
                "accountTypeCode": account_type_code,
                "accountTypeName": account_type_name,

                "balanceAmount": balance,
                "currency": currency,
                "iban": iban,

                "accountStatus": acc.get("accountStatus", "") or "",
                "lockedForDebit": bool(acc.get("lockedForDebit", False)),
                "lockedForCredit": bool(acc.get("lockedForCredit", False)),
            }

            # Add SERVER_TIMESTAMP only for Firestore (not serializable to JSON)
            firestore_data = {**trimmed, "syncedAt": firestore.SERVER_TIMESTAMP}

            acc_ref = accounts_ref.document(account_id)
            batch.set(acc_ref, firestore_data, merge=True)
            out.append(trimmed)

        batch.commit()

        return {"status": "success", "accounts_synced": len(out), "accounts": out}

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Accounts API error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync error: {str(e)}")


@app.get("/get_transactions/{uid}/{account_id}")
def get_transactions(uid: str, account_id: str):
    """
    Fetch transactions for an account and store them in Firestore under:
    users/{uid}/accounts/{account_id}/transactions/{transactionId}

    - Do NOT send skip/limit/sort to JOPACC (400 error).
    - If a transactionId already exists in Firestore, we SKIP it
      (no update / no overwrite).
    """

    url = f"{TRANS_BASE_URL}/{account_id}/transactions"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        body = response.json()
        transactions = body.get("data", [])

        account_ref = (
            db.collection("users")
              .document(uid)
              .collection("accounts")
              .document(account_id)
        )
        tx_ref = account_ref.collection("transactions")

        existing_ids = {doc.id for doc in tx_ref.stream()}

        batch = db.batch()
        count = 0

        for tx in transactions:
            tx_id = str(tx.get("transactionId") or "").strip()
            if not tx_id:
                continue

            if tx_id in existing_ids:
                continue

            # Amount + currency
            raw_amount = tx.get("transactionAmount", {}).get("amount", 0.0)
            try:
                amount = float(raw_amount)
            except (TypeError, ValueError):
                amount = 0.0

            currency = (
                tx.get("transactionAmount", {})
                  .get("currency", "JOD")
            )

            # Type
            ttype = (tx.get("transactionType") or "").lower()
            if ttype not in ("debit", "credit"):
                ttype = "debit"

            # Date (keep as ISO string)
            settlement_dt = tx.get("settlementDateTime")

            # Merchant / "from where"
            creditor = (tx.get("creditor") or {}).get("creditorPersonal") or {}
            debtor = (tx.get("debtor") or {}).get("debtorPersonal") or {}
            merchant = creditor.get("name") or debtor.get("name") or None
            if isinstance(merchant, str):
                merchant = merchant.strip() or None

            # Description from rmtInf.unstructured[0]
            description = None
            rmt = tx.get("rmtInf") or {}
            unstructured = rmt.get("unstructured")
            if isinstance(unstructured, list) and unstructured:
                description = str(unstructured[0])

            # Account label from IBAN last 4
            debtor_account = (tx.get("debtor") or {}).get("debtorAccount") or {}
            main_route = debtor_account.get("mainRoute") or {}
            iban = main_route.get("address")
            account_label = None
            if isinstance(iban, str) and len(iban) >= 4:
                account_label = "•••• " + iban[-4:]

            doc_ref = tx_ref.document(tx_id)

            doc_data = {
                "accountId": account_id,
                "amount": amount,
                "currency": currency,
                "type": ttype,
                "date": settlement_dt,
                "merchantName": merchant,
                "description": description,
                "accountLabel": account_label,
                "source": "openBanking",
                "category": None,
            }

            batch.set(doc_ref, doc_data, merge=False)
            count += 1

        batch.commit()

        return {
            "status": "success",
            "transactions_synced": count,
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"Transactions API error: {str(e)}"
        )

@app.get("/get_sosps/{uid}/{account_id}") 
def get_sosps(uid: str, account_id: str):
    url = f"{SOSP_BASE_URL}/accounts/{account_id}/SOSPs"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        sosps = response.json().get("data", [])

        account_ref = db.collection("users").document(uid).collection("accounts").document(account_id)
        sosp_ref = account_ref.collection("sosps")

        for doc in sosp_ref.stream():
            doc.reference.delete()

        batch = db.batch()
        for sosp in sosps:
            sosp_doc = sosp_ref.document(sosp["SOSPId"])
            batch.set(sosp_doc, sosp)
        batch.commit()

        return {
            "status": "success",
            "sosps_synced": len(sosps)
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"SOSPs API error: {str(e)}")

#########################################################################################################################

@app.get("/subscribe")
def subscribe(email: str):
    """
    """
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email address.")

    db = firestore.client()

    subs_ref = db.collection("subscriptions").document(email)
    subs_ref.set({
        "email": email,
        "subscribedAt": firestore.SERVER_TIMESTAMP,
    })

    return {"status": "success", "message": f"Subscription successful for {email}."}


# --- Uvicorn Entrypoint ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render sets PORT automatically
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
