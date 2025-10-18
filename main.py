from fastapi import FastAPI, HTTPException
import requests
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
import uvicorn

# --- Firebase Setup ---
firebase_key = os.environ.get("FIREBASE_KEY")
if not firebase_key:
    raise Exception("FIREBASE_KEY environment variable not set.")

cred = credentials.Certificate(json.loads(firebase_key))
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

@app.get("/")
def root():
    return {"message": "✅ Vesta backend is live and running 🚀"}

# --- Base URLs ---
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
        accounts = response.json().get("data", [])

        user_ref = db.collection("users").document(uid)
        accounts_ref = user_ref.collection("accounts")

        # Step 1: Delete existing accounts
        for doc in accounts_ref.stream():
            doc.reference.delete()

        # Step 2: Add new accounts and calculate totals
        batch = db.batch()
        total_balance = 0.0
        total_savings = 0.0

        for acc in accounts:
            balance = acc.get("availableBalance", {}).get("balanceAmount", 0.0)
            total_balance += balance

            # Detect savings accounts
            account_type_code = acc.get("accountType", {}).get("code", "").upper()
            account_type_name = acc.get("accountType", {}).get("name", "").lower()
            if "SAV" in account_type_code or "savings" in account_type_name:
                total_savings += balance

            acc_ref = accounts_ref.document(acc["accountId"])
            batch.set(acc_ref, acc)

        # Step 3: Update user document totals
        user_updates = {
            "totalBalance": total_balance,
            "currency": accounts[0]["accountCurrency"] if accounts else "JOD",
        }
        if total_savings > 0:
            user_updates["totalSavings"] = total_savings

        batch.update(user_ref, user_updates)
        batch.commit()

        return {
            "status": "success",
            "accounts_synced": len(accounts),
            "totalBalance": total_balance,
            "totalSavings": total_savings,
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Accounts API error: {str(e)}")


# --- Get Transactions Endpoint ---
@app.get("/get_transactions/{uid}/{account_id}")
def get_transactions(uid: str, account_id: str):
    url = f"{TRANS_BASE_URL}/{account_id}/transactions"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        transactions = response.json().get("data", [])

        account_ref = db.collection("users").document(uid).collection("accounts").document(account_id)
        tx_ref = account_ref.collection("transactions")

        # Clear old transactions
        for doc in tx_ref.stream():
            doc.reference.delete()

        # Save new transactions
        batch = db.batch()
        for tx in transactions:
            tx_doc = tx_ref.document(tx["transactionId"])
            batch.set(tx_doc, tx)
        batch.commit()

        return {
            "status": "success",
            "transactions_synced": len(transactions)
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Transactions API error: {str(e)}")

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
            sosp_doc = sosp_ref.document(sosp["sospId"])
            batch.set(sosp_doc, sosp)
        batch.commit()

        return {
            "status": "success",
            "sosps_synced": len(sosps)
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"SOSPs API error: {str(e)}")


# --- Uvicorn Entrypoint ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render sets PORT automatically
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
