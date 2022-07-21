python3 -c '
import jwt
import uuid
import datetime
from datetime import timezone
from Crypto.PublicKey import RSA
import json
from authlib.jose import jwk


kid = str(uuid.uuid4())
key_pair = RSA.generate(2048)
private_key = key_pair.export_key()
public_key = key_pair.publickey().export_key()


payload = {
  "id": str(uuid.uuid4()),
  "on_behalf_of": "my_user",
  "exp": datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(seconds=30),
  "iss": "dolthub.com",
  "sub": "test_jwt_user",
  "aud": ["my_resource"],
}
token = jwt.encode(
    payload,
    private_key,
    algorithm="RS256",
    headers={"kid": kid},
)

jwtfile = open("token.jwt", "w")
jwtfile.write(token)

jwks = {"keys": [jwk.dumps(public_key, kty="RSA", kid=kid)]}
with open("test-jwks.json", "w") as f:
    json.dump(jwks, f)
'