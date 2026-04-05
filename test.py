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


String token = await FirebaseAuth.instance.currentUser!.getIdToken(true); // true = force refresh
print(token);
