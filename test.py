import requests
import base64

# Make sure these are the EXACT strings from the top section of your Finto dashboard
CLIENT_ID = "22a312fb-4d8d-4c41-9b49-fff15658d496"
CLIENT_SECRET = "Xjysex7o0rFeV52ZFgudoRhsefYNKF8OYL2qx3sTqw933MM+djNOy8+NfwiLXEiLh6skMcJ3DfPaP3tFOuXx0A=="
URL = "https://api.developer.bankaletihad.com/api/v1/tppa/token"

def test_etihad_auth():
    print("Preparing Basic Auth and mTLS certificates...")
    
    # 1. Securely encode the Client ID and Secret
    creds = f"{CLIENT_ID.strip()}:{CLIENT_SECRET.strip()}"
    encoded_creds = base64.b64encode(creds.encode('utf-8')).decode('utf-8')
    
    headers = {
        "Authorization": f"Basic {encoded_creds}",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Vesta-Backend/1.0"
    }
    
    # 2. Only send the grant type and scope in the body
    data = {
        "grant_type": "client_credentials",
        "scope": "accounts"
    }
    
    print(f"Sending POST request to {URL}...")
    try:
        r = requests.post(
            URL, 
            headers=headers, 
            data=data, 
            # 3. Attach the mTLS certificates
            cert=('signedCert__314.crt', 'server.key'), 
            timeout=10
        )
        print(f"\n--- RESULTS ---")
        print(f"Status Code: {r.status_code}")
        
        if r.status_code == 200:
            print("SUCCESS! The API accepted your credentials.")
            print(r.json())
        else:
            print("FAILED.")
            print(f"Response Body: {r.text}")
            
    except Exception as e:
        print(f"Request failed to send: {e}")

if __name__ == "__main__":
    test_etihad_auth()