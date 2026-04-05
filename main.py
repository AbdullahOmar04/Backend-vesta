import base64
from datetime import datetime, timedelta, timezone
import hashlib
import time
import uuid
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware  # ADD THIS
from pydantic import BaseModel
import jwt
import requests
import firebase_admin
from firebase_admin import auth, credentials, firestore
import os
import json
import uvicorn
from urllib.parse import urlencode

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
is_production = os.getenv("ENVIRONMENT") == "production"

app = FastAPI(
    title="Vesta Backend",
    description="MVP backend for syncing accounts and transactions with Firebase",
    version="0.1.0",
    docs_url=None if is_production else "/docs",
    redoc_url=None if is_production else "/redoc",
    openapi_url=None if is_production else "/openapi.json",
)

# --- CORS Configuration --- ADD THIS ENTIRE SECTION
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://vesta-83939.web.app",
        "https://vesta-83939.firebaseapp.com",
        "https://vestaapp.co",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_bearer = HTTPBearer()

def get_authenticated_uid(credentials: HTTPAuthorizationCredentials = Depends(_bearer)) -> str:
    try:
        decoded = auth.verify_id_token(credentials.credentials)
        return decoded["uid"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired Firebase token")


@app.get("/")
def root(_uid: str = Depends(get_authenticated_uid)):
    return {"message": "✅ Vesta backend is live and running 🚀"}


#################### JOPACC INTEGRATION ENDPOINTS ####################
ACC_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Accounts/v0.4.3"
TRANS_BASE_URL = "http://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Transactions/v0.4.3/accounts"
SOSP_BASE_URL = "https://jpcjofsdev.apigw-az-eu.webmethods.io/gateway/Standing%20Orders%20&%20Scheduled%20Payments%20(SOSPs)/v0.4.3"


# --- Sync Accounts Endpoint ---
@app.get("/sync_accounts/{uid}/{customer_id}")
def sync_accounts(uid: str, customer_id: str, _uid: str = Depends(get_authenticated_uid)):
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

        # Get already-linked account IDs so we don't overwrite their linked status
        linked_ids = set()
        for doc in accounts_ref.where("linked", "==", True).stream():
            linked_ids.add(doc.id)

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

            # Only set linked=False for NEW accounts; preserve existing linked status
            if account_id not in linked_ids:
                trimmed["linked"] = False

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
def get_transactions(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
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
def get_sosps(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
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

_subscribe_attempts: dict[str, list[float]] = {}
_SUBSCRIBE_LIMIT = 5
_SUBSCRIBE_WINDOW = 3600.0  # 1 hour

@app.get("/subscribe")
def subscribe(email: str, request: Request):
    ip = request.client.host
    now = time.time()
    window_start = now - _SUBSCRIBE_WINDOW
    attempts = [t for t in _subscribe_attempts.get(ip, []) if t > window_start]
    if len(attempts) >= _SUBSCRIBE_LIMIT:
        raise HTTPException(status_code=429, detail="Too many subscription attempts. Try again later.")
    attempts.append(now)
    _subscribe_attempts[ip] = attempts

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email address.")

    subs_ref = db.collection("subscriptions").document(email)
    subs_ref.set({
        "email": email,
        "subscribedAt": firestore.SERVER_TIMESTAMP,
    })

    return {"status": "success", "message": f"Subscription successful for {email}."}



#####################################################AHLI BANK ##########################################################
COMPLY_HOST = os.getenv("COMPLY_HOST", "https://jo-comply.thefinx.io").rstrip("/")

AHLI_CLIENT_ID = os.getenv("AHLI_CLIENT_ID", "")
AHLI_CLIENT_SECRET = os.getenv("AHLI_CLIENT_SECRET", "")

# Required by most Comply calls
FINX_INSTITUTION_APP_CODE = os.getenv("FINX_INSTITUTION_APP_CODE", "")  # e.g. "xxxx"
# Your backend callback (must match what you registered on Comply)
AHLI_REDIRECT_URI = os.getenv("AHLI_REDIRECT_URI", "")  # e.g. "https://backend-vesta.onrender.com/banks/ahli/callback"

# RSA private key (PEM) for signing the JWT request parameter in the authorize URL
VESTA_AHLI_SIGNING_KEY = os.getenv("VESTA_AHLI_SIGNING_KEY", "")

# OpenID discovery: if Comply has a specific endpoint in Postman collection, set it here.
# If empty, we fall back to standard .well-known path (may or may not work for your tenant).
FINX_OPENID_CONFIG_URL = os.getenv(
    "FINX_OPENID_CONFIG_URL",
    f"{COMPLY_HOST}/.well-known/openid-configuration",
)

# Comply API base (the “/api/public/jo/v0.4/...” calls in your screenshots)
COMPLY_API_BASE = os.getenv("COMPLY_API_BASE", f"{COMPLY_HOST}/api/public/jo/v0.4").rstrip("/")

AHLI_AUTHORIZE_URL = os.getenv("AHLI_AUTHORIZE_URL", f"{COMPLY_HOST}/sandbox/{FINX_INSTITUTION_APP_CODE}/authorize")

AHLI_PROVIDER_KEY = "ahli"
AHLI_PROVIDER_LABEL = "Ahli"
AHLI_SANDBOX = "finx"  # store alongside accounts like your other sandbox keys


# ----------------------------
# Helpers
# ----------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _require_env() -> None:
    missing = []
    if not AHLI_CLIENT_ID: missing.append("AHLI_CLIENT_ID")
    if not AHLI_CLIENT_SECRET: missing.append("AHLI_CLIENT_SECRET")
    if not FINX_INSTITUTION_APP_CODE: missing.append("FINX_INSTITUTION_APP_CODE")
    if not AHLI_REDIRECT_URI: missing.append("AHLI_REDIRECT_URI")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")

def _tpp_client_credentials_token() -> dict:
    """
    Token for calling Comply “TPP” endpoints (client_credentials).
    """
    _require_env()
    url = f"{COMPLY_HOST}/keycloak/realms/open-banking/protocol/openid-connect/token"
    data = {"grant_type": "client_credentials"}
    r = requests.post(url, data=data, auth=(AHLI_CLIENT_ID, AHLI_CLIENT_SECRET), timeout=20)
    r.raise_for_status()
    return r.json()

def _openid_config(tpp_access_token: str | None = None) -> dict:
    r = requests.get(
        f"{COMPLY_API_BASE}/.well-known/openid-configuration",
        headers={
            "Authorization": f"Bearer {tpp_access_token}" if tpp_access_token else "",
            "x-ftg-institution-application-code": FINX_INSTITUTION_APP_CODE,
            "Accept": "application/json",
            "x-interactions-id": str(uuid.uuid4()),
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def _create_consent(tpp_access_token: str, permissions: list[str], expiration_dt: datetime) -> dict:
    """
    Creates account-access-consent for PSU.
    Postman sandbox typically accepts JSON body.
    """
    url = f"{COMPLY_API_BASE}/account-access-consents"
    interaction_id = str(uuid.uuid4())

    headers = {
        "Authorization": f"Bearer {tpp_access_token}",
        "x-ftg-institution-application-code": FINX_INSTITUTION_APP_CODE,
        "x-interactions-id": interaction_id,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {
        "permissions": permissions,
        "expirationDateTime": _iso(expiration_dt),
    }

    r = requests.post(url, headers=headers, json=payload, timeout=25)
    r.raise_for_status()
    return r.json()


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")

def _pkce_pair() -> tuple[str, str]:
    verifier = _b64url(os.urandom(32))
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge

def _build_auth_url(openid_conf: dict, *, consent_id: str, state: str, code_challenge: str) -> str:
    """
    FINX/Comply JO sandbox authorize:
    - Must use authorization_endpoint from openid configuration:
        https://jo-comply.thefinx.io/sandbox/<institution_app_code>/authorize
    - Include PKCE (S256)
    - Include a SIGNED `request` JWT containing openbanking_intent_id + standard OIDC claims
    """
    _require_env()

    auth_ep = (openid_conf.get("authorization_endpoint") or "").strip()
    if not auth_ep:
        raise HTTPException(status_code=500, detail="OpenID config missing authorization_endpoint")

    if not VESTA_AHLI_SIGNING_KEY:
        raise HTTPException(status_code=500, detail="Missing env var: VESTA_AHLI_SIGNING_KEY")

    issuer = (openid_conf.get("issuer") or "").strip()
    if not issuer:
        raise HTTPException(status_code=500, detail="OpenID config missing issuer")

    nonce = uuid.uuid4().hex
    now = int(time.time())

    # IMPORTANT: For FINX sandbox authorize, audience commonly expected is the sandbox authorize origin,
    # not the token issuer. We set aud to BOTH to be safe.
    aud = [issuer, auth_ep.split("/sandbox/")[0]]  # e.g. [".../realms/open-banking", "https://jo-comply.thefinx.io"]

    request_obj = {
        "iss": AHLI_CLIENT_ID,
        "aud": aud,
        "response_type": "code",
        "client_id": AHLI_CLIENT_ID,
        "redirect_uri": AHLI_REDIRECT_URI,
        "scope": "openid accounts",
        "state": state,
        "nonce": nonce,

        # intent
        "openbanking_intent_id": consent_id,

        # PKCE must match outer params (some servers validate inside request too)
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",

        "iat": now,
        "exp": now + 300,
        "jti": uuid.uuid4().hex,
    }

    request_jwt = jwt.encode(request_obj, VESTA_AHLI_SIGNING_KEY, algorithm="RS256")

    params = {
        "client_id": AHLI_CLIENT_ID,
        "scope": "openid accounts",
        "response_type": "code",
        "redirect_uri": AHLI_REDIRECT_URI,
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",

        # ✅ required for this sandbox authorize
        "request": request_jwt,
    }

    return f"{auth_ep}?{urlencode(params)}"

def _exchange_code_for_psu_token(openid_conf: dict, *, code: str, code_verifier: str) -> dict:
    token_ep = openid_conf.get("token_endpoint") or f"{COMPLY_HOST}/keycloak/realms/open-banking/protocol/openid-connect/token"
    data = {
    "grant_type": "authorization_code",
    "client_id": AHLI_CLIENT_ID,
    "code": code,
    "redirect_uri": AHLI_REDIRECT_URI,
    "code_verifier": code_verifier,
    }
    r = requests.post(token_ep, data=data, auth=(AHLI_CLIENT_ID, AHLI_CLIENT_SECRET), timeout=25)
    r.raise_for_status()
    return r.json()

def _refresh_psu_token(openid_conf: dict, *, refresh_token: str) -> dict:
    token_ep = openid_conf.get("token_endpoint") or f"{COMPLY_HOST}/keycloak/realms/open-banking/protocol/openid-connect/token"
    data = {
    "grant_type": "refresh_token",
    "client_id": AHLI_CLIENT_ID,
    "refresh_token": refresh_token,
    }
    r = requests.post(token_ep, data=data, auth=(AHLI_CLIENT_ID, AHLI_CLIENT_SECRET), timeout=25)
    r.raise_for_status()
    return r.json()

def _providers_ref(uid: str):
    return db.collection("users").document(uid)

def _write_provider_state(uid: str, data: dict):
    user_ref = _providers_ref(uid)
    user_ref.set(
        {"providers": {AHLI_PROVIDER_KEY: data}},
        merge=True
    )

def _read_provider_state(uid: str) -> dict:
    snap = _providers_ref(uid).get()
    d = snap.to_dict() or {}
    return ((d.get("providers") or {}).get(AHLI_PROVIDER_KEY) or {})

def _ensure_psu_access_token(uid: str) -> str:
    """
    Loads stored PSU token; refreshes if expired/near-expiry.
    """
    st = _read_provider_state(uid)
    tokens = st.get("tokens") or {}
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at")  # ISO string
    if not access_token:
        raise HTTPException(status_code=400, detail="Ahli not linked yet (no access_token). Start link first.")

    # If no expiry stored, just use it
    if not expires_at:
        return access_token

    try:
        exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except Exception:
        return access_token

    # Refresh if expiring within 60s
    if exp <= (_utc_now() + timedelta(seconds=60)) and refresh_token:
        tpp = _tpp_client_credentials_token()
        openid_conf = _openid_config(tpp.get("access_token"))
        newt = _refresh_psu_token(openid_conf, refresh_token=refresh_token)

        new_access = newt.get("access_token", access_token)
        new_refresh = newt.get("refresh_token", refresh_token)
        ttl = int(newt.get("expires_in") or 0)
        new_exp = _utc_now() + timedelta(seconds=max(ttl, 0))

        _write_provider_state(uid, {
            "sandbox": AHLI_SANDBOX,
            "tokens": {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "expires_at": _iso(new_exp),
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }
        })
        return new_access

    return access_token

def _comply_headers_psu(psu_access_token: str) -> dict:
    """
    Minimal headers that usually satisfy sandbox.
    Add more if your tenant enforces them strictly.
    """
    return {
        "Authorization": f"Bearer {psu_access_token}",
        "x-ftg-institution-application-code": FINX_INSTITUTION_APP_CODE,
        "Accept": "application/json",
        "x-interactions-id": str(uuid.uuid4()),
    }


# ----------------------------
# ENDPOINTS
# ----------------------------

@app.get("/banks/ahli/start_link/{uid}")
def ahli_start_link(uid: str, _uid: str = Depends(get_authenticated_uid)):
    """
    1) Get TPP token (client_credentials)
    2) Create consent
    3) Build auth URL for user to open in WebView/browser
    """
    _require_env()

    permissions = [
        "readAccounts",
        "readBalances",
        "readTransactions",
        "readTransactionsCredits",
        "readTransactionsDebits",
        "readBeneficiaries",
    ]
    expires = _utc_now() + timedelta(days=365)

    tpp = _tpp_client_credentials_token()
    tpp_access = tpp.get("access_token", "")
    if not tpp_access:
        raise HTTPException(status_code=500, detail="Failed to get TPP access_token")

    openid_conf = _openid_config(tpp_access)

    consent = _create_consent(tpp_access, permissions=permissions, expiration_dt=expires)
    consent_id = consent.get("consentId") or consent.get("consent_id")
    if not consent_id:
        raise HTTPException(status_code=500, detail=f"Consent response missing consentId: {consent}")

    state = uuid.uuid4().hex
    code_verifier, code_challenge = _pkce_pair()

    _write_provider_state(uid, {
        "provider": AHLI_PROVIDER_LABEL,
        "sandbox": AHLI_SANDBOX,
        "consentId": consent_id,
        "state": state,
        "status": consent.get("status"),
        "permissions": permissions,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    db.collection("linkSessions").document(state).set({
        "uid": uid,
        "consentId": consent_id,
        "code_verifier": code_verifier,   # ✅ REQUIRED
        "createdAt": firestore.SERVER_TIMESTAMP,
    })

    auth_url = _build_auth_url(
        openid_conf,
        consent_id=consent_id,
        state=state,
        code_challenge=code_challenge,     # ✅ REQUIRED
    )

    return {"status": "ok", "consentId": consent_id, "authUrl": auth_url}


@app.get("/banks/ahli/callback", response_class=HTMLResponse)
def ahli_callback(request: Request):
    _require_env()

    qp = dict(request.query_params)
    code = (qp.get("code") or "").strip()
    state = (qp.get("state") or "").strip()
    error = (qp.get("error") or "").strip()
    error_desc = (qp.get("error_description") or "").strip()

    if error:
        return HTMLResponse(f"Link failed: {error} {error_desc}", status_code=400)

    if not code or not state:
        return HTMLResponse("Missing code/state", status_code=400)

    session_ref = db.collection("linkSessions").document(state)
    session_snap = session_ref.get()
    if not session_snap.exists:
        return HTMLResponse("Invalid or expired state", status_code=400)

    session = session_snap.to_dict() or {}

    uid = (session.get("uid") or "").strip()
    if not uid:
        return HTMLResponse("Invalid session: missing uid", status_code=400)

    code_verifier = (session.get("code_verifier") or "").strip()
    if not code_verifier:
        return HTMLResponse("Invalid session: missing code_verifier", status_code=400)

    st = _read_provider_state(uid)
    expected_state = (st.get("state") or "").strip()
    if not expected_state or expected_state != state:
        return HTMLResponse("Invalid state", status_code=400)

    tpp = _tpp_client_credentials_token()
    openid_conf = _openid_config(tpp.get("access_token"))

    tokens = _exchange_code_for_psu_token(
        openid_conf,
        code=code,
        code_verifier=code_verifier,
    )

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    ttl = int(tokens.get("expires_in") or 0)
    exp = _utc_now() + timedelta(seconds=max(ttl, 0))

    if not access_token:
        return HTMLResponse(f"Token exchange failed: {tokens}", status_code=400)

    _write_provider_state(uid, {
        "provider": AHLI_PROVIDER_LABEL,
        "sandbox": AHLI_SANDBOX,
        "consentId": st.get("consentId"),
        "state": None,
        "tokens": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": _iso(exp),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        "linked": True,
        "linkedAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    session_ref.delete()

    try:
        _ahli_sync_accounts_internal(uid)
    except Exception:
        pass

    return HTMLResponse(
        "<h3>✅ Ahli linked successfully.</h3><p>You can close this window and return to Vesta.</p>",
        status_code=200,
    )


def _ahli_sync_accounts_internal(uid: str) -> dict:
    psu_token = _ensure_psu_access_token(uid)
    headers = _comply_headers_psu(psu_token)

    url = f"{COMPLY_API_BASE}/accounts"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()

    body = r.json()
    accounts = body.get("data") if isinstance(body, dict) else None
    if accounts is None and isinstance(body, list):
        accounts = body
    if accounts is None:
        accounts = []

    user_ref = db.collection("users").document(uid)
    accounts_ref = user_ref.collection("accounts")

    # Get already-linked Ahli account IDs so we don't overwrite their linked status
    linked_ids = set()
    for doc in accounts_ref.stream():
        d = doc.to_dict() or {}
        if d.get("provider") == AHLI_PROVIDER_LABEL and d.get("sandbox") == AHLI_SANDBOX:
            if d.get("linked") == True:
                linked_ids.add(doc.id)
            else:
                doc.reference.delete()

    batch = db.batch()
    total_balance = 0.0
    currency = "JOD"

    for acc in accounts:
        account_id = str(acc.get("accountId") or "").strip()
        if not account_id:
            continue

        # balance can be under availableBalance.amount OR availableBalance.balanceAmount depending on schema
        bal_obj = acc.get("availableBalance") or {}
        raw_bal = bal_obj.get("amount")
        if raw_bal is None:
            raw_bal = bal_obj.get("balanceAmount")
        try:
            bal = float(raw_bal) if raw_bal is not None else 0.0
        except Exception:
            bal = 0.0

        total_balance += bal
        currency = acc.get("accountCurrency") or currency

        acc_doc = {
            **acc,
            "provider": AHLI_PROVIDER_LABEL,
            "sandbox": AHLI_SANDBOX,
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        # Only set linked=False for NEW accounts; preserve existing linked status
        if account_id not in linked_ids:
            acc_doc["linked"] = False

        batch.set(accounts_ref.document(account_id), acc_doc, merge=True)

    # update user totals for this provider (keep it provider-scoped to avoid mixing with JoPACC totals)
    batch.set(
        user_ref,
        {
            "providers": {
                AHLI_PROVIDER_KEY: {
                    "totals": {
                        "totalBalance": total_balance,
                        "currency": currency,
                        "updatedAt": firestore.SERVER_TIMESTAMP,
                    }
                }
            }
        },
        merge=True,
    )

    batch.commit()

    return {"accounts_synced": len(accounts), "totalBalance": total_balance, "currency": currency}


@app.get("/banks/ahli/sync_accounts/{uid}")
def ahli_sync_accounts(uid: str, _uid: str = Depends(get_authenticated_uid)):
    """
    Call after the callback (or manually) to refresh accounts from Ahli via Comply.
    """
    out = _ahli_sync_accounts_internal(uid)
    return {"status": "success", **out}


@app.get("/banks/ahli/get_account/{uid}/{account_id}")
def ahli_get_account(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    """
    Fetch a single account by id from Comply.
    GET {{comply-host}}/api/public/jo/v0.4/accounts/{accountId}
    """
    psu_token = _ensure_psu_access_token(uid)
    headers = _comply_headers_psu(psu_token)
    headers["accountSchema"] = "accountId"

    url = f"{COMPLY_API_BASE}/accounts/{account_id}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()

    return {"status": "success", "data": r.json()}


@app.get("/banks/ahli/get_balances/{uid}/{account_id}")
def ahli_get_balances(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    """
    Fetch all balances of an account from Comply.
    GET {{comply-host}}/api/public/jo/v0.4/accounts/{accountId}/balances
    """
    psu_token = _ensure_psu_access_token(uid)
    headers = _comply_headers_psu(psu_token)

    url = f"{COMPLY_API_BASE}/accounts/{account_id}/balances"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()

    return {"status": "success", "data": r.json()}


@app.get("/banks/ahli/get_transactions/{uid}/{account_id}")
def ahli_get_transactions(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    """
    Fetch transactions for one account and store:
      users/{uid}/accounts/{account_id}/transactions/{transactionId}
    Skips existing transactionIds.
    """
    psu_token = _ensure_psu_access_token(uid)
    headers = _comply_headers_psu(psu_token)

    url = f"{COMPLY_API_BASE}/accounts/{account_id}/transactions"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()

    body = r.json()
    txs = body.get("data") if isinstance(body, dict) else None
    if txs is None and isinstance(body, list):
        txs = body
    if txs is None:
        txs = []

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

    for tx in txs:
        tx_id = str(tx.get("transactionId") or "").strip()
        if not tx_id or tx_id in existing_ids:
            continue

        amt_obj = tx.get("transactionAmount") or {}
        raw_amount = amt_obj.get("amount")
        try:
            amount = float(raw_amount) if raw_amount is not None else 0.0
        except Exception:
            amount = 0.0

        currency = amt_obj.get("currency") or "JOD"

        ttype = (tx.get("transactionType") or "debit").lower()
        if ttype not in ("debit", "credit"):
            ttype = "debit"

        settlement_dt = tx.get("settlementDateTime") or tx.get("presentementDateTime") or tx.get("presentmentDateTime")

        creditor = (tx.get("creditor") or {}).get("creditorPersonal") or {}
        debtor = (tx.get("debtor") or {}).get("debtorPersonal") or {}
        merchant = creditor.get("name") or debtor.get("name") or None
        if isinstance(merchant, str):
            merchant = merchant.strip() or None

        description = None
        rmt = tx.get("rmtInf") or {}
        if isinstance(rmt, dict):
            unstructured = rmt.get("unstructured")
            if isinstance(unstructured, list) and unstructured:
                description = str(unstructured[0])
        elif isinstance(rmt, str) and rmt.strip():
            description = rmt.strip()

        doc_data = {
            "accountId": account_id,
            "amount": amount,
            "currency": currency,
            "type": ttype,
            "date": settlement_dt,
            "merchantName": merchant,
            "description": description,
            "source": "openBanking",
            "category": None,
            "provider": AHLI_PROVIDER_LABEL,
            "sandbox": AHLI_SANDBOX,
            "raw": tx,
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        batch.set(tx_ref.document(tx_id), doc_data, merge=False)
        count += 1

    batch.commit()

    return {"status": "success", "transactions_synced": count}


#####################################################CAPITAL BANK (CBOJ) ##########################################################
# Capital Bank has its own sandbox (not FINX Comply)
CBOJ_CLIENT_ID = os.getenv("CBOJ_CLIENT_ID", "")
CBOJ_CLIENT_SECRET = os.getenv("CBOJ_CLIENT_SECRET", "")
CBOJ_REDIRECT_URI = os.getenv("CBOJ_REDIRECT_URI", "")
CBOJ_SANDBOX_HOST = os.getenv("CBOJ_SANDBOX_HOST", "https://sandbox.api.capitalbank.jo:8448").rstrip("/")
CBOJ_API_BASE = os.getenv("CBOJ_API_BASE", f"{CBOJ_SANDBOX_HOST}/ob/api/ais").rstrip("/")
CBOJ_TOKEN_URL = os.getenv("CBOJ_TOKEN_URL", f"{CBOJ_SANDBOX_HOST}/ob/oauth2/token")      # TPP client_credentials
CBOJ_PSU_TOKEN_URL = os.getenv("CBOJ_PSU_TOKEN_URL", CBOJ_TOKEN_URL)                        # PSU auth code exchange
CBOJ_AUTHORIZE_URL = os.getenv("CBOJ_AUTHORIZE_URL", f"{CBOJ_SANDBOX_HOST}/ob/web/login")

CBOJ_PROVIDER_KEY = "capital"
CBOJ_PROVIDER_LABEL = "Capital"
CBOJ_SANDBOX = "capitalbank"


# ----------------------------
# CBOJ Helpers (Capital Bank direct sandbox — NOT FINX Comply)
# ----------------------------
def _cboj_require_env() -> None:
    missing = []
    if not CBOJ_CLIENT_ID: missing.append("CBOJ_CLIENT_ID")
    if not CBOJ_CLIENT_SECRET: missing.append("CBOJ_CLIENT_SECRET")
    if not CBOJ_REDIRECT_URI: missing.append("CBOJ_REDIRECT_URI")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def _cboj_tpp_token() -> dict:
    """Client credentials token from Capital Bank's own OAuth server."""
    _cboj_require_env()
    data = {
        "grant_type": "client_credentials",
        "client_id": CBOJ_CLIENT_ID,
        "client_secret": CBOJ_CLIENT_SECRET,
        "scope": "accounts",
    }
    r = requests.post(CBOJ_TOKEN_URL, data=data, timeout=20, verify=False)
    r.raise_for_status()
    return r.json()


def _cboj_create_consent(tpp_access_token: str, permissions: list[str],
                         tx_from: datetime, tx_to: datetime) -> dict:
    """
    POST /account-access-consents
    CBOJ spec requires transactionFromDateTime + transactionToDateTime.
    Response returns consentRef (not consentId).
    """
    url = f"{CBOJ_API_BASE}/account-access-consents"
    headers = {
        "Authorization": f"Bearer {tpp_access_token}",
        "x-interactions-id": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "transactionFromDate": _iso(tx_from),
        "transactionToDate": _iso(tx_to),
        "permissions": permissions,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=25, verify=False)
    r.raise_for_status()
    return r.json()


def _cboj_build_auth_url(
    *,
    consent_ref: str,
    state: str,
    login_url: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str = "S256",
) -> str:
    _cboj_require_env()

    extra = {
        "client_id": CBOJ_CLIENT_ID,
        "response_type": "code",
        "scope": "accounts",
        "redirect_uri": CBOJ_REDIRECT_URI,
        "state": state,
    }

    # ✅ PKCE
    if code_challenge:
        extra["code_challenge"] = code_challenge
        extra["code_challenge_method"] = code_challenge_method

    if login_url:
        separator = "&" if "?" in login_url else "?"
        return f"{login_url}{separator}{urlencode(extra)}"

    extra["consentRef"] = consent_ref
    return f"{CBOJ_AUTHORIZE_URL}?{urlencode(extra)}"



def _cboj_exchange_code(*, code: str) -> dict:
    """Exchange authorization code for PSU tokens.
    Per docs: same endpoint as client_credentials (sandbox.api.capitalbank.jo:8448).
    No PKCE. Requires scope=accounts.
    """
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CBOJ_CLIENT_ID,
        "client_secret": CBOJ_CLIENT_SECRET,
        "scope": "accounts",
        "redirect_uri": CBOJ_REDIRECT_URI,
    }

    r = requests.post(
        CBOJ_TOKEN_URL,          # same host as TPP token per docs
        data=data,
        timeout=25,
        verify=False,
    )

    if r.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {r.status_code} {r.text[:500]}")

    return r.json()


def _cboj_refresh_token(*, refresh_token: str) -> dict:
    """Refresh an expired PSU access token."""
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CBOJ_CLIENT_ID,
        "client_secret": CBOJ_CLIENT_SECRET,
        "scope": "accounts",
        "redirect_uri": CBOJ_REDIRECT_URI,
    }
    r = requests.post(CBOJ_TOKEN_URL, data=data, timeout=25, verify=False)
    r.raise_for_status()
    return r.json()


def _cboj_headers_psu(psu_access_token: str) -> dict:
    """Headers for CBOJ data APIs. Per YAML spec, x-interactions-id and
    x-idempotency-key are both required."""
    return {
        "Authorization": f"Bearer {psu_access_token}",
        "Accept": "application/json",
        "x-interactions-id": str(uuid.uuid4()),
        "x-idempotency-key": str(uuid.uuid4()),
    }


def _cboj_providers_ref(uid: str):
    return db.collection("users").document(uid)


def _cboj_write_provider_state(uid: str, data: dict):
    user_ref = _cboj_providers_ref(uid)
    user_ref.set(
        {"providers": {CBOJ_PROVIDER_KEY: data}},
        merge=True,
    )


def _cboj_read_provider_state(uid: str) -> dict:
    snap = _cboj_providers_ref(uid).get()
    d = snap.to_dict() or {}
    return ((d.get("providers") or {}).get(CBOJ_PROVIDER_KEY) or {})


def _cboj_ensure_psu_token(uid: str) -> str:
    """Load stored PSU token for CBOJ; refresh if expired."""
    st = _cboj_read_provider_state(uid)
    tokens = st.get("tokens") or {}
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at")

    if not access_token:
        raise HTTPException(status_code=400, detail="Capital Bank not linked yet. Start link first.")

    if not expires_at:
        return access_token

    try:
        exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except Exception:
        return access_token

    if exp <= (_utc_now() + timedelta(seconds=60)) and refresh_token:
        newt = _cboj_refresh_token(refresh_token=refresh_token)

        new_access = newt.get("access_token", access_token)
        new_refresh = newt.get("refresh_token", refresh_token)
        ttl = int(newt.get("expires_in") or 0)
        new_exp = _utc_now() + timedelta(seconds=max(ttl, 0))

        _cboj_write_provider_state(uid, {
            "sandbox": CBOJ_SANDBOX,
            "tokens": {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "expires_at": _iso(new_exp),
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }
        })
        return new_access

    return access_token


# ----------------------------
# CBOJ ENDPOINTS
# ----------------------------

@app.get("/banks/capital/start_link/{uid}")
def capital_start_link(uid: str, _uid: str = Depends(get_authenticated_uid)):
    """
    1) Get TPP token (client_credentials)
    2) Create consent with CBOJ-spec permissions
    3) Build auth URL for user to open in WebView/browser
    """
    _cboj_require_env()

    # PascalCase permissions per CBOJ YAML spec
    permissions = [
        "ReadAccounts",
        "ReadBalances",
        "ReadTransactions",
        "ReadBeneficiaries",
        "ReadStandingOrders&ScheduledPayments",
    ]
    tx_from = datetime(2000, 1, 1, tzinfo=timezone.utc)
    tx_to = datetime(2052, 12, 2, tzinfo=timezone.utc)

    try:
        tpp = _cboj_tpp_token()
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"TPP token failed: {e.response.status_code} {e.response.text[:500]}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TPP token error ({type(e).__name__}): {e}")

    tpp_access = tpp.get("access_token", "")
    if not tpp_access:
        raise HTTPException(status_code=500, detail=f"No access_token in TPP response: {tpp}")

    try:
        consent = _cboj_create_consent(tpp_access, permissions=permissions,
                                       tx_from=tx_from, tx_to=tx_to)
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Consent creation failed: {e.response.status_code} {e.response.text[:500]}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Consent creation error ({type(e).__name__}): {e}")

    # CBOJ uses consentRef (not consentId)
    consent_ref = consent.get("consentRef") or consent.get("consentId")
    if not consent_ref:
        raise HTTPException(status_code=500, detail=f"Consent response missing consentRef: {consent}")

    # Extract loginUrl from consent response (CBOJ spec includes loginUrls[])
    login_urls = consent.get("loginUrls") or []
    login_url = login_urls[0] if login_urls else None

    state = uuid.uuid4().hex

    _cboj_write_provider_state(uid, {
        "provider": CBOJ_PROVIDER_LABEL,
        "sandbox": CBOJ_SANDBOX,
        "consentRef": consent_ref,
        "state": state,
        "status": consent.get("status"),
        "permissions": permissions,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    db.collection("linkSessions").document(state).set({
        "uid": uid,
        "bank": CBOJ_PROVIDER_KEY,
        "consentRef": consent_ref,
        "createdAt": firestore.SERVER_TIMESTAMP,
    })

    auth_url = _cboj_build_auth_url(
        consent_ref=consent_ref,
        state=state,
        login_url=login_url,
    )

    return {"status": "ok", "consentRef": consent_ref, "authUrl": auth_url}


@app.get("/banks/capital/callback", response_class=HTMLResponse)
def capital_callback(request: Request):
    _cboj_require_env()

    qp = dict(request.query_params)
    code = (qp.get("code") or "").strip()
    state = (qp.get("state") or "").strip()
    error = (qp.get("error") or "").strip()
    error_desc = (qp.get("error_description") or "").strip()

    if error:
        return HTMLResponse(f"Link failed: {error} {error_desc}", status_code=400)
    if not code or not state:
        return HTMLResponse("Missing code/state", status_code=400)

    session_ref = db.collection("linkSessions").document(state)
    session_snap = session_ref.get()
    if not session_snap.exists:
        return HTMLResponse("Invalid or expired state", status_code=400)

    session = session_snap.to_dict() or {}
    uid = (session.get("uid") or "").strip()
    if not uid:
        return HTMLResponse("Invalid session: missing uid", status_code=400)

    st = _cboj_read_provider_state(uid)
    expected_state = (st.get("state") or "").strip()
    if not expected_state or expected_state != state:
        return HTMLResponse("Invalid state", status_code=400)

    try:
        tokens = _cboj_exchange_code(code=code)
    except requests.exceptions.HTTPError as e:
        return HTMLResponse(
            f"Token exchange failed: {e.response.status_code} {e.response.text[:500]}",
            status_code=400,
        )
    except Exception as e:
        return HTMLResponse(f"Token exchange error: {e}", status_code=400)

    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    ttl = int(tokens.get("expires_in") or 0)
    exp = _utc_now() + timedelta(seconds=max(ttl, 0))

    if not access_token:
        return HTMLResponse(f"Token exchange returned no access_token: {tokens}", status_code=400)

    _cboj_write_provider_state(uid, {
        "provider": CBOJ_PROVIDER_LABEL,
        "sandbox": CBOJ_SANDBOX,
        "consentRef": st.get("consentRef"),
        "state": None,
        "tokens": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": _iso(exp),
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        "linked": True,
        "linkedAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    session_ref.delete()

    try:
        _cboj_sync_accounts_internal(uid)
    except Exception:
        pass

    return HTMLResponse(
        "<h3>Capital Bank linked successfully.</h3><p>You can close this window and return to Vesta.</p>",
        status_code=200,
    )


def _cboj_sync_accounts_internal(uid: str) -> dict:
    #Fetch accounts from CBOJ and store in Firestore.
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts"
    r = requests.get(url, headers=headers, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code,
                            detail=f"Capital Bank accounts error: {r.status_code} {r.text[:300]}")

    body = r.json()
    accounts = body.get("data") if isinstance(body, dict) else None
    if accounts is None and isinstance(body, list):
        accounts = body
    if accounts is None:
        accounts = []

    user_ref = db.collection("users").document(uid)
    accounts_ref = user_ref.collection("accounts")

    # Delete only Capital Bank accounts
    for doc in accounts_ref.stream():
        d = doc.to_dict() or {}
        if d.get("provider") == CBOJ_PROVIDER_LABEL and d.get("sandbox") == CBOJ_SANDBOX:
            doc.reference.delete()

    batch = db.batch()
    total_balance = 0.0
    currency = "JOD"

    for acc in accounts:
        account_id = str(acc.get("accountId") or "").strip()
        if not account_id:
            continue

        # CBOJ schema: availableBalance is {amount, currency}
        bal_obj = acc.get("availableBalance") or {}
        raw_bal = bal_obj.get("amount")
        try:
            bal = float(raw_bal) if raw_bal is not None else 0.0
        except Exception:
            bal = 0.0

        total_balance += bal
        currency = (bal_obj.get("currency") or acc.get("currencies") or currency)

        # IBAN from mainRoute.address
        main_route = acc.get("mainRoute") or {}
        iban = main_route.get("address") or ""

        # Bank/institution name
        inst = acc.get("institutionBasicInfo") or {}
        bank_name = ((inst.get("name") or {}).get("enName") or "Capital Bank")

        acc_type = acc.get("accountType") or {}

        acc_doc = {
            "accountId": account_id,
            "provider": CBOJ_PROVIDER_LABEL,
            "sandbox": CBOJ_SANDBOX,
            "linked": False,
            "bankName": bank_name,
            "accountTypeCode": acc_type.get("code", ""),
            "accountTypeName": acc_type.get("name", ""),
            "balanceAmount": bal,
            "currency": currency,
            "iban": iban,
            "accountStatus": acc.get("status", ""),
            "lockedForDebit": bool(acc.get("lockedForDebit", False)),
            "isSharedAccount": bool(acc.get("isSharedAccount", False)),
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        batch.set(accounts_ref.document(account_id), acc_doc, merge=True)

    batch.set(
        user_ref,
        {
            "providers": {
                CBOJ_PROVIDER_KEY: {
                    "totals": {
                        "totalBalance": total_balance,
                        "currency": currency,
                        "updatedAt": firestore.SERVER_TIMESTAMP,
                    }
                }
            }
        },
        merge=True,
    )
    batch.commit()

    return {"accounts_synced": len(accounts), "totalBalance": total_balance, "currency": currency}


@app.get("/banks/capital/sync_accounts/{uid}")
def capital_sync_accounts(uid: str, _uid: str = Depends(get_authenticated_uid)):
    # Refresh accounts from Capital Bank
    out = _cboj_sync_accounts_internal(uid)
    return {"status": "success", **out}


@app.get("/banks/capital/get_account/{uid}/{account_id}")
def capital_get_account(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    # GET /accounts/{accountId}
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_account_by_iban/{uid}/{iban}")
def capital_get_account_by_iban(uid: str, iban: str, _uid: str = Depends(get_authenticated_uid)):
    # GET /accounts/address/{accountAddress}?accountSchema=IBAN
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/address/{iban}"
    r = requests.get(url, headers=headers, params={"accountSchema": "IBAN"}, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_balances/{uid}/{account_id}")
def capital_get_balances(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    # GET /accounts/{accountId}/balances
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}/balances"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_transactions/{uid}/{account_id}")
def capital_get_transactions(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
    
    #Fetch transactions from CBOJ and store in Firestore.
    #CBOJ schema: amount={amount,currency}, transactionDirection=credit/debit.
    
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}/transactions"
    r = requests.get(url, headers=headers, timeout=25)
    # Capital Bank returns 404 or 500 when account has no transactions
    if r.status_code in (404, 500) or r.status_code >= 500:
        return {"status": "success", "transactions_synced": 0}
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code,
                            detail=f"Capital Bank transactions error for {account_id}: {r.status_code} {r.text[:300]}")

    body = r.json()
    txs = body.get("data") if isinstance(body, dict) else None
    if txs is None and isinstance(body, list):
        txs = body
    if txs is None:
        txs = []

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

    for tx in txs:
        tx_id = str(tx.get("transactionId") or "").strip()
        if not tx_id or tx_id in existing_ids:
            continue

        # CBOJ schema: amount is {amount, currency}
        amt_obj = tx.get("amount") or {}
        raw_amount = amt_obj.get("amount")
        try:
            amount = float(raw_amount) if raw_amount is not None else 0.0
        except Exception:
            amount = 0.0
        currency = amt_obj.get("currency") or "JOD"

        # CBOJ uses transactionDirection (credit/debit)
        direction = (tx.get("transactionDirection") or "debit").lower()
        if direction not in ("debit", "credit"):
            direction = "debit"

        settlement_dt = tx.get("settlementDateTime") or tx.get("presentementDateTime")

        doc_data = {
            "accountId": account_id,
            "amount": amount,
            "currency": currency,
            "type": direction,
            "date": settlement_dt,
            "transactionType": tx.get("transactionType"),
            "status": tx.get("status"),
            "source": "openBanking",
            "category": None,
            "provider": CBOJ_PROVIDER_LABEL,
            "sandbox": CBOJ_SANDBOX,
            "raw": tx,
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        batch.set(tx_ref.document(tx_id), doc_data, merge=False)
        count += 1

    batch.commit()
    return {"status": "success", "transactions_synced": count}


@app.get("/banks/capital/get_transaction/{uid}/{account_id}/{transaction_id}")
def capital_get_transaction(uid: str, account_id: str, transaction_id: str, _uid: str = Depends(get_authenticated_uid)):
    #GET /accounts/{accountId}/transactions/{transactionId
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}/transactions/{transaction_id}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_beneficiaries/{uid}")
def capital_get_beneficiaries(uid: str, _uid: str = Depends(get_authenticated_uid)):
    #GET /beneficiaries
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/beneficiaries"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_beneficiary/{uid}/{beneficiary_id}")
def capital_get_beneficiary(uid: str, beneficiary_id: str, _uid: str = Depends(get_authenticated_uid)):
    #]GET /beneficiaries/{beneficiaryId 
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/beneficiaries/{beneficiary_id}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_sosps/{uid}/{account_id}")
def capital_get_sosps(uid: str, account_id: str, _uid: str = Depends(get_authenticated_uid)):
     #GET /accounts/{accountId}/sosp - Standing Orders & Scheduled Payments
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}/sosp"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_sosp/{uid}/{account_id}/{sosp_id}")
def capital_get_sosp(uid: str, account_id: str, sosp_id: str, _uid: str = Depends(get_authenticated_uid)):
    #GET /accounts/{accountId}/sosp/{sospId 
    psu_token = _cboj_ensure_psu_token(uid)
    headers = _cboj_headers_psu(psu_token)

    url = f"{CBOJ_API_BASE}/accounts/{account_id}/sosp/{sosp_id}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.get("/banks/capital/get_consent/{uid}")
def capital_get_consent(uid: str, _uid: str = Depends(get_authenticated_uid)):
    #GET /account-access-consents/{consentRef} - Check consent status
    st = _cboj_read_provider_state(uid)
    consent_ref = st.get("consentRef")
    if not consent_ref:
        raise HTTPException(status_code=400, detail="No consent found for this user")

    tpp = _cboj_tpp_token()
    tpp_access = tpp.get("access_token", "")
    headers = {
        "Authorization": f"Bearer {tpp_access}",
        "Accept": "application/json",
        "x-interactions-id": str(uuid.uuid4()),
    }

    url = f"{CBOJ_API_BASE}/account-access-consents/{consent_ref}"
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return {"status": "success", "data": r.json()}


@app.delete("/banks/capital/revoke_consent/{uid}")
def capital_revoke_consent(uid: str, _uid: str = Depends(get_authenticated_uid)):
    #DELETE /account-access-consents/{consentRef} - Revoke consent
    st = _cboj_read_provider_state(uid)
    consent_ref = st.get("consentRef")
    if not consent_ref:
        raise HTTPException(status_code=400, detail="No consent found for this user")

    tpp = _cboj_tpp_token()
    tpp_access = tpp.get("access_token", "")
    headers = {
        "Authorization": f"Bearer {tpp_access}",
        "Accept": "application/json",
        "x-interactions-id": str(uuid.uuid4()),
    }

    url = f"{CBOJ_API_BASE}/account-access-consents/{consent_ref}"
    r = requests.delete(url, headers=headers, timeout=25)
    r.raise_for_status()

    _cboj_write_provider_state(uid, {
        "linked": False,
        "consentRef": None,
        "tokens": None,
        "revokedAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    return {"status": "success", "message": "Consent revoked"}


####################################################  ETIHAD BANK (Bank Al Etihad — Finto platform)  ####################################################
# Etihad uses a completely different architecture from CBOJ/Ahli:
#   - Direct login flow (username + password + OTP), NO browser redirect
#   - Identity API: /token, /token2FA/otp, /token2FA
#   - Accounts API: /customers, /customers/{id}/accounts, /customers/{id}/accounts/{num}/transactions
#   - Basic Auth for /token endpoint, Bearer JWT for everything else

ETIHAD_CLIENT_ID = os.getenv("ETIHAD_CLIENT_ID", "")
ETIHAD_CLIENT_SECRET = os.getenv("ETIHAD_CLIENT_SECRET", "")
ETIHAD_IDENTITY_BASE = os.getenv("ETIHAD_IDENTITY_BASE", "https://api.developer.bankaletihad.com/api/v1/tppa").rstrip("/")
ETIHAD_ACCOUNTS_BASE = os.getenv("ETIHAD_ACCOUNTS_BASE", "https://api.developer.bankaletihad.com/api/v1/partner/accounts").rstrip("/")

ETIHAD_CERT_FILE = os.getenv("ETIHAD_CERT_FILE", "signedCert__314.crt")
ETIHAD_KEY_FILE = os.getenv("ETIHAD_KEY_FILE", "server.key")
ETIHAD_MTLS = (ETIHAD_CERT_FILE, ETIHAD_KEY_FILE)

ETIHAD_PROVIDER_KEY = "etihad"
ETIHAD_PROVIDER_LABEL = "Etihad"
ETIHAD_SANDBOX = "bankaletihad"


# ----------------------------
# Etihad Helpers
# ----------------------------
def _etihad_require_env() -> None:
    missing = []
    if not ETIHAD_CLIENT_ID: missing.append("ETIHAD_CLIENT_ID")
    if not ETIHAD_CLIENT_SECRET: missing.append("ETIHAD_CLIENT_SECRET")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


def _etihad_basic_auth_header() -> str:
    """Basic Auth header value for /token endpoint: base64(client_id:client_secret)."""
    creds = f"{ETIHAD_CLIENT_ID}:{ETIHAD_CLIENT_SECRET}"
    encoded = base64.b64encode(creds.encode()).decode()
    return f"Basic {encoded}"


def _etihad_tpp_token() -> dict:
    """Get TPP-level access token via client_credentials grant."""
    _etihad_require_env()
    url = f"{ETIHAD_IDENTITY_BASE}/token"
    
    headers = {
        "Authorization": _etihad_basic_auth_header(),
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Vesta-Backend/1.0"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": ETIHAD_CLIENT_ID.strip(),
        "scope": "identity accounts",
    }
    
    # Add cert=ETIHAD_MTLS
    r = requests.post(url, headers=headers, data=data, cert=ETIHAD_MTLS, timeout=20)
    
    if r.status_code >= 400:
        print(f"TPP TOKEN FAIL: {r.status_code} - {r.text}")
    r.raise_for_status()
    return r.json()


def _etihad_password_login(*, username: str, password: str) -> dict:
    """Direct login via POST /token with password grant (no 2FA).
    Used when the user has no 2FA configured (sandbox test users)."""
    _etihad_require_env()
    url = f"{ETIHAD_IDENTITY_BASE}/token"
    headers = {
        "Authorization": _etihad_basic_auth_header(),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "password",
        "client_id": ETIHAD_CLIENT_ID.strip(),
        "username": username,
        "password": password,
        "scope": "accounts",
    }
    r = requests.post(url, headers=headers, data=data, cert=ETIHAD_MTLS, timeout=20)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Password login failed: {r.text[:500]}")
    return r.json()


def _etihad_login_init(*, username: str, password: str, tpp_access_token: str) -> dict:
    """POST /token2FA/otp — triggers OTP to user's phone.
    Returns 200 (token if no 2FA) or 202 (OTP sent).
    Falls back to password grant if 2FA is not configured (406)."""
    url = f"{ETIHAD_IDENTITY_BASE}/token2FA/otp"
    headers = {
        "Authorization": f"Bearer {tpp_access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "Username": username,
        "Password": password,
        "Scope": "accounts",
    }
    r = requests.post(url, headers=headers, json=payload, cert=ETIHAD_MTLS, timeout=20)

    if r.status_code == 202:
        return {"status": "otp_sent"}
    if r.status_code == 200:
        return {"status": "authenticated", "tokens": r.json()}

    # 406 = 2FA not configured → fall back to direct password grant
    if r.status_code == 406:
        tok = _etihad_password_login(username=username, password=password)
        return {"status": "authenticated", "tokens": tok}

    raise HTTPException(status_code=r.status_code, detail=f"Login init failed: {r.text[:500]}")


def _etihad_login_complete(*, username: str, password: str, otp_code: str, tpp_access_token: str) -> dict:
    """POST /token2FA — complete 2FA with OTP code, returns access_token."""
    url = f"{ETIHAD_IDENTITY_BASE}/token2FA"
    headers = {
        "Authorization": f"Bearer {tpp_access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "Username": username,
        "Password": password,
        "Scope": "openid accounts",
        "Code": otp_code,
    }
    r = requests.post(url, headers=headers, json=payload, cert=ETIHAD_MTLS, timeout=20)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Login complete failed: {r.text[:500]}")
    return r.json()


def _etihad_refresh_token(*, refresh_token: str) -> dict:
    """Refresh an expired user access token."""
    _etihad_require_env()
    url = f"{ETIHAD_IDENTITY_BASE}/token"
    headers = {
        "Authorization": _etihad_basic_auth_header(),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "client_id": ETIHAD_CLIENT_ID,
        "refresh_token": refresh_token,
    }
    r = requests.post(url, headers=headers, data=data, cert=ETIHAD_MTLS, timeout=20)
    r.raise_for_status()
    return r.json()


def _etihad_headers(access_token: str) -> dict:
    """Bearer headers for Etihad data APIs."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


# --- Firestore helpers ---
def _etihad_providers_ref(uid: str):
    return db.collection("users").document(uid)


def _etihad_write_provider_state(uid: str, data: dict):
    _etihad_providers_ref(uid).set(
        {"providers": {ETIHAD_PROVIDER_KEY: data}},
        merge=True,
    )


def _etihad_read_provider_state(uid: str) -> dict:
    snap = _etihad_providers_ref(uid).get()
    d = snap.to_dict() or {}
    return ((d.get("providers") or {}).get(ETIHAD_PROVIDER_KEY) or {})


def _etihad_ensure_token(uid: str) -> str:
    """Load stored user token for Etihad; refresh if expired."""
    st = _etihad_read_provider_state(uid)
    tokens = st.get("tokens") or {}
    access_token = tokens.get("access_token")
    refresh_tok = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at")

    if not access_token:
        raise HTTPException(status_code=400, detail="Etihad Bank not linked yet. Login first.")

    if not expires_at:
        return access_token

    try:
        exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except Exception:
        return access_token

    if exp <= (_utc_now() + timedelta(seconds=60)) and refresh_tok:
        newt = _etihad_refresh_token(refresh_token=refresh_tok)

        new_access = newt.get("access_token", access_token)
        new_refresh = newt.get("refresh_token", refresh_tok)
        ttl = int(newt.get("expires_in") or 0)
        new_exp = _utc_now() + timedelta(seconds=max(ttl, 0))

        _etihad_write_provider_state(uid, {
            "sandbox": ETIHAD_SANDBOX,
            "linked": True,
            "tokens": {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "expires_at": _iso(new_exp),
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }
        })
        return new_access

    return access_token


# ----------------------------
# Etihad ENDPOINTS
# ----------------------------

class EtihadCreateUserBody(BaseModel):
    username: str
    password: str
    email: str
    first_name: str
    last_name: str
    phone_number: str

class EtihadLoginInitBody(BaseModel):
    username: str
    password: str

class EtihadLoginCompleteBody(BaseModel):
    otp: str
    username: str | None = None
    password: str | None = None


@app.post("/banks/etihad/create_user")
def etihad_create_user(body: EtihadCreateUserBody, _uid: str = Depends(get_authenticated_uid)):
    """Create a sandbox test user on the Finto platform.
    POST /users — requires TPP token with 'identity' scope."""
    _etihad_require_env()

    try:
        tpp = _etihad_tpp_token()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TPP token error: {e}")

    tpp_access = tpp.get("access_token", "")
    if not tpp_access:
        raise HTTPException(status_code=500, detail="No access_token in TPP response")

    url = f"{ETIHAD_IDENTITY_BASE}/users"
    headers = {
        "Authorization": f"Bearer {tpp_access}",
        "Content-Type": "application/json",
        "Idempotency-key": str(uuid.uuid4()),
    }
    payload = {
        "Username": body.username,
        "Password": body.password,
        "Email": body.email,
        "FirstName": body.first_name,
        "LastName": body.last_name,
        "PhoneNumber": body.phone_number,
    }

    r = requests.post(url, headers=headers, json=payload, cert=ETIHAD_MTLS, timeout=20)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Create user failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


@app.post("/banks/etihad/login_init/{uid}")
def etihad_login_init(uid: str, body: EtihadLoginInitBody, _uid: str = Depends(get_authenticated_uid)):
    """Step 1: Send username + password → triggers OTP to user's phone."""
    _etihad_require_env()

    username = body.username.strip()
    password = body.password.strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")

    try:
        tpp = _etihad_tpp_token()
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"TPP token failed: {e.response.status_code} {e.response.text[:500]}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TPP token error ({type(e).__name__}): {e}")

    tpp_access = tpp.get("access_token", "")
    if not tpp_access:
        raise HTTPException(status_code=500, detail=f"No access_token in TPP response: {tpp}")

    result = _etihad_login_init(username=username, password=password, tpp_access_token=tpp_access)

    # Store TPP token + credentials temporarily for login_complete
    _etihad_write_provider_state(uid, {
        "provider": ETIHAD_PROVIDER_LABEL,
        "sandbox": ETIHAD_SANDBOX,
        "tpp_access_token": tpp_access,
        "pending_username": username,
        "pending_password": password,
        "status": result.get("status"),
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    # If no 2FA, user is directly authenticated
    if result.get("status") == "authenticated":
        tok = result["tokens"]
        ttl = int(tok.get("expires_in") or 0)
        exp = _utc_now() + timedelta(seconds=max(ttl, 0))
        _etihad_write_provider_state(uid, {
            "provider": ETIHAD_PROVIDER_LABEL,
            "sandbox": ETIHAD_SANDBOX,
            "linked": True,
            "tokens": {
                "access_token": tok.get("access_token"),
                "refresh_token": tok.get("refresh_token"),
                "expires_at": _iso(exp),
            },
            "status": "authenticated",
            "updatedAt": firestore.SERVER_TIMESTAMP,
        })
        return {"status": "authenticated", "message": "Logged in directly (no 2FA required)"}

    return {"status": "otp_sent", "message": "OTP sent to your phone. Call login_complete with the OTP code."}


@app.post("/banks/etihad/login_complete/{uid}")
def etihad_login_complete(uid: str, body: EtihadLoginCompleteBody, _uid: str = Depends(get_authenticated_uid)):
    """Step 2: Verify OTP and get access token.
    Body: {"otp": "123456"} — username/password are retrieved from stored state.
    Optionally: {"username": "...", "password": "...", "otp": "123456"}
    """
    _etihad_require_env()

    otp_code = body.otp.strip()
    if not otp_code:
        raise HTTPException(status_code=400, detail="otp is required")

    st = _etihad_read_provider_state(uid)

    # Use stored credentials or body overrides
    username = ((body.username or "").strip() or st.get("pending_username") or "").strip()
    password = ((body.password or "").strip() or st.get("pending_password") or "").strip()
    tpp_access = st.get("tpp_access_token", "")

    if not username or not password:
        raise HTTPException(status_code=400, detail="No pending credentials found. Call login_init first.")
    if not tpp_access:
        # Get a fresh TPP token
        try:
            tpp = _etihad_tpp_token()
            tpp_access = tpp.get("access_token", "")
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"TPP token error: {e}")

    tok = _etihad_login_complete(
        username=username, password=password,
        otp_code=otp_code, tpp_access_token=tpp_access,
    )

    ttl = int(tok.get("expires_in") or 0)
    exp = _utc_now() + timedelta(seconds=max(ttl, 0))

    _etihad_write_provider_state(uid, {
        "provider": ETIHAD_PROVIDER_LABEL,
        "sandbox": ETIHAD_SANDBOX,
        "linked": True,
        "tokens": {
            "access_token": tok.get("access_token"),
            "refresh_token": tok.get("refresh_token"),
            "expires_at": _iso(exp),
        },
        "status": "authenticated",
        "pending_username": None,
        "pending_password": None,
        "tpp_access_token": None,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    return {"status": "success", "message": "Etihad Bank linked successfully"}


@app.get("/banks/etihad/get_customers/{uid}")
def etihad_get_customers(uid: str, _uid: str = Depends(get_authenticated_uid)):
    """GET /customers — list all customers linked to the app.
    Uses app token (client_credentials), not user token."""
    _etihad_require_env()
    try:
        tpp = _etihad_tpp_token()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"TPP token error: {e}")

    headers = _etihad_headers(tpp.get("access_token", ""))
    url = f"{ETIHAD_ACCOUNTS_BASE}/customers"
    r = requests.get(url, headers=headers, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get customers failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


class EtihadCreateAccountBody(BaseModel):
    currency: str = "JOD"
    name: str


@app.post("/banks/etihad/create_account/{uid}/{customer_id}")
def etihad_create_account(uid: str, customer_id: str, body: EtihadCreateAccountBody, _uid: str = Depends(get_authenticated_uid)):
    """POST /customers/{customerId}/accounts — create a new account for a customer."""
    access_token = _etihad_ensure_token(uid)
    headers = {
        **_etihad_headers(access_token),
        "Content-Type": "application/json",
        "Idempotency-key": str(uuid.uuid4()),
    }

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts"
    payload = {"Currency": body.currency, "Name": body.name}

    r = requests.post(url, headers=headers, json=payload, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Create account failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


@app.get("/banks/etihad/get_accounts/{uid}/{customer_id}")
def etihad_get_accounts(uid: str, customer_id: str, _uid: str = Depends(get_authenticated_uid)):
    """GET /customers/{customerId}/accounts — list all accounts for a customer."""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts"
    r = requests.get(url, headers=headers, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get accounts failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


@app.get("/banks/etihad/get_account/{uid}/{customer_id}/{account_number}")
def etihad_get_account(uid: str, customer_id: str, account_number: str, _uid: str = Depends(get_authenticated_uid)):
    """GET /customers/{customerId}/accounts/{accountNumber} — single account details."""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts/{account_number}"
    r = requests.get(url, headers=headers, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get account failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


@app.get("/banks/etihad/get_transactions/{uid}/{customer_id}/{account_number}")
def etihad_get_transactions(
    uid: str, customer_id: str, account_number: str,
    date_from: str | None = None, date_to: str | None = None,
    page_number: int = 1, page_size: int = 50,
    _uid: str = Depends(get_authenticated_uid),
):
    """GET /customers/{customerId}/accounts/{accountNumber}/transactions"""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts/{account_number}/transactions"
    params: dict = {"pageNumber": page_number, "pageSize": page_size}
    if date_from:
        params["dateFrom"] = date_from
    if date_to:
        params["dateTo"] = date_to

    r = requests.get(url, headers=headers, params=params, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get transactions failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


@app.get("/banks/etihad/get_exchange_rate/{uid}/{customer_id}")
def etihad_get_exchange_rate(
    uid: str, customer_id: str,
    from_currency: str = "USD", to_currency: str = "JOD",
    amount: str = "1", type: str = "CREDIT",
    customer_account: str | None = None,
    _uid: str = Depends(get_authenticated_uid),
):
    """GET /customers/{customerId}/payments/exchangeRates"""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/payments/exchangeRates"
    params: dict = {
        "fromCurrencyCode": from_currency,
        "toCurrencyCode": to_currency,
        "fromCurrencyAmount": amount,
        "type": type,
    }
    if customer_account:
        params["customerAccount"] = customer_account

    r = requests.get(url, headers=headers, params=params, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get exchange rate failed: {r.text[:500]}")
    return {"status": "success", "data": r.json()}


# --- Sync endpoints ---

@app.post("/banks/etihad/sync_accounts/{uid}/{customer_id}")
def etihad_sync_accounts(uid: str, customer_id: str, _uid: str = Depends(get_authenticated_uid)):
    """Fetch all accounts for a customer and sync to Firestore."""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts"
    r = requests.get(url, headers=headers, cert=ETIHAD_MTLS, timeout=25)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=f"Get accounts failed: {r.text[:500]}")

    accounts = r.json()
    if not isinstance(accounts, list):
        accounts = [accounts] if accounts else []

    batch = db.batch()
    acct_ref = db.collection("users").document(uid).collection("accounts")
    count = 0

    for acct in accounts:
        acct_number = acct.get("Number") or acct.get("IBAN") or str(uuid.uuid4())
        acct_id = f"etihad_{acct_number}"

        doc_data = {
            "accountId": acct_id,
            "accountNumber": acct.get("Number"),
            "iban": acct.get("IBAN"),
            "swift": acct.get("SWIFT"),
            "name": acct.get("Name"),
            "bankName": acct.get("BankName"),
            "branch": acct.get("Branch"),
            "currency": acct.get("CurrencyCode"),
            "currentBalance": acct.get("CurrentBalance"),
            "availableBalance": acct.get("AvailableBalance"),
            "blockedAmount": acct.get("BlockedAmount"),
            "noDebit": acct.get("NoDebit"),
            "noCredit": acct.get("NoCredit"),
            "dormant": acct.get("Dormant"),
            "customerName": (acct.get("Customer") or {}).get("Name"),
            "customerId": customer_id,
            "source": "openBanking",
            "provider": ETIHAD_PROVIDER_LABEL,
            "sandbox": ETIHAD_SANDBOX,
            "raw": acct,
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        batch.set(acct_ref.document(acct_id), doc_data, merge=False)
        count += 1

    batch.commit()
    return {"status": "success", "accounts_synced": count}


@app.post("/banks/etihad/sync_transactions/{uid}/{customer_id}/{account_number}")
def etihad_sync_transactions(
    uid: str, customer_id: str, account_number: str,
    date_from: str | None = None, date_to: str | None = None,
    _uid: str = Depends(get_authenticated_uid),
):
    """Fetch transactions and sync to Firestore."""
    access_token = _etihad_ensure_token(uid)
    headers = _etihad_headers(access_token)

    acct_id = f"etihad_{account_number}"

    # Paginate through all transactions
    all_transactions = []
    page = 1
    page_size = 100
    while True:
        url = f"{ETIHAD_ACCOUNTS_BASE}/customers/{customer_id}/accounts/{account_number}/transactions"
        params: dict = {"pageNumber": page, "pageSize": page_size}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        r = requests.get(url, headers=headers, params=params, cert=ETIHAD_MTLS, timeout=25)
        if r.status_code >= 400:
            if page == 1:
                raise HTTPException(status_code=r.status_code, detail=f"Get transactions failed: {r.text[:500]}")
            break

        txns = r.json()
        if not isinstance(txns, list):
            txns = [txns] if txns else []

        if not txns:
            break

        all_transactions.extend(txns)
        if len(txns) < page_size:
            break
        page += 1

    batch = db.batch()
    tx_ref = db.collection("users").document(uid).collection("transactions")
    count = 0

    for tx in all_transactions:
        # Build a stable ID from transaction fields
        tx_date = tx.get("Date", "")
        tx_amount = str(tx.get("TransactionAmount", ""))
        tx_desc = tx.get("Description", "")
        tx_indicator = tx.get("DebitCreditIndicator", "")
        raw_id = f"{account_number}_{tx_date}_{tx_amount}_{tx_indicator}_{tx_desc}"
        tx_id = hashlib.sha256(raw_id.encode()).hexdigest()[:20]

        direction = "credit" if tx_indicator == "C" else "debit"
        amount = tx.get("TransactionAmount", 0)
        currency = tx.get("TransactionCurrency", "JOD")

        doc_data = {
            "accountId": acct_id,
            "amount": amount,
            "currency": currency,
            "type": direction,
            "date": tx.get("Date"),
            "valueDate": tx.get("ValueDate"),
            "description": tx_desc,
            "source": "openBanking",
            "category": None,
            "provider": ETIHAD_PROVIDER_LABEL,
            "sandbox": ETIHAD_SANDBOX,
            "raw": tx,
            "syncedAt": firestore.SERVER_TIMESTAMP,
        }

        batch.set(tx_ref.document(tx_id), doc_data, merge=False)
        count += 1

    batch.commit()
    return {"status": "success", "transactions_synced": count}


@app.delete("/banks/etihad/logout/{uid}")
def etihad_logout(uid: str, _uid: str = Depends(get_authenticated_uid)):
    """Logout user session and clear stored tokens."""
    st = _etihad_read_provider_state(uid)
    tokens = st.get("tokens") or {}
    access_token = tokens.get("access_token")

    # Call Etihad logout endpoint if we have a token
    if access_token:
        try:
            url = f"{ETIHAD_IDENTITY_BASE}/token"
            headers = {"Authorization": f"Bearer {access_token}"}
            requests.delete(url, headers=headers, cert=ETIHAD_MTLS, timeout=10)
        except Exception:
            pass  # Best effort

    _etihad_write_provider_state(uid, {
        "linked": False,
        "tokens": None,
        "status": "logged_out",
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })

    return {"status": "success", "message": "Logged out from Etihad Bank"}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # Render sets PORT automatically
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
