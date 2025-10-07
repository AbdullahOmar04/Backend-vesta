from fastapi import FastAPI, HTTPException
import requests
import firebase_admin
from firebase_admin import credentials, firestore

import os, json

firebase_key = os.environ.get("FIREBASE_KEY")
if not firebase_key:
    raise Exception("FIREBASE_KEY environment variable not set.")
cred = credentials.Certificate(json.loads(firebase_key))

firebase_admin.initialize_app(cred)
db = firestore.client()

app = FastAPI(
    title="Vesta Backend",
    description="MVP backend for syncing accounts and transactions with Firebase",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.get("/")
def root():
    return {"message": "Vesta backend is live 🚀"}

ACC_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Accounts/v0.4.3"
TRANS_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Transactions/v0.4.3/accounts"

@app.get("/sync_accounts/{uid}/{customer_id}")
def sync_accounts(uid: str, customer_id: str):
    url = f"{ACC_BASE_URL}/accounts"
    headers = {"x-customer-id": customer_id}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        accounts = response.json().get("data", [])

        user_ref = db.collection("users").document(uid)
        accounts_ref = user_ref.collection("accounts")

        # Step 1: Delete existing accounts
        existing = accounts_ref.stream()
        for doc in existing:
            doc.reference.delete()

        # Step 2: Add new accounts and calculate totals
        batch = db.batch()
        total_balance = 0.0
        total_savings = 0.0

        for acc in accounts:
            balance = acc.get("availableBalance", {}).get("balanceAmount", 0.0)
            total_balance += balance

            # ✅ Detect savings account based on accountType code or name
            account_type_code = acc.get("accountType", {}).get("code", "").upper()
            account_type_name = acc.get("accountType", {}).get("name", "").lower()

            if "SAV" in account_type_code or "savings" in account_type_name:
                total_savings += balance

            # Save full account data
            acc_ref = accounts_ref.document(acc["accountId"])
            batch.set(acc_ref, acc)

        # Step 3: Update aggregate fields on user document
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
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_transactions/{uid}/{account_id}")
def get_transactions(uid: str, account_id: str):
    url = f"{TRANS_BASE_URL}/{account_id}/transactions"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        transactions = response.json().get("data", [])

        account_ref = db.collection("users").document(uid).collection("accounts").document(account_id)
        tx_ref = account_ref.collection("transactions")

        old_docs = tx_ref.stream()
        for doc in old_docs:
            doc.reference.delete()

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
        raise HTTPException(status_code=500, detail=str(e))