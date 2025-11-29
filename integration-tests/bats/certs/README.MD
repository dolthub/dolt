The certs and keys in this directory are used for testing cert-based authentication with a running server. 
Each cert is set to expire in 100 years. The rest of this file documents the steps used to generate them. 

CA private key and cert:
```sh
openssl genrsa 2048 > ca-key.pem

openssl req -new -x509 -nodes -days 36500 \
    -key ca-key.pem -out ca.pem \
    -subj "/C=US/ST=Washington/L=Seattle/O=Test CA/CN=MySQL Test CA"
```

Server private key and cert:
```sh
openssl genrsa 2048 > server-key.pem

openssl req -new -key server-key.pem -out server-req.pem \
    -subj "/C=US/ST=Washington/L=Seattle/O=Test Server/CN=localhost"

openssl x509 -req -in server-req.pem -days 36500 \
    -CA ca.pem -CAkey ca-key.pem -set_serial 01 \
    -out server-cert.pem
```

Client private key and cert:
```sh
openssl genrsa 2048 > client-key.pem

openssl req -new -key client-key.pem -out client-req.pem \
    -subj "/C=US/ST=Washington/L=Seattle/O=Test Client/CN=testclient"

openssl x509 -req -in client-req.pem -days 36500 \
    -CA ca.pem -CAkey ca-key.pem -set_serial 02 \
    -out client-cert.pem
```

Alternate CA and client private keys and certs:
```sh
openssl req -x509 -newkey rsa:2048 -days 36500 -keyout alt-ca-key.pem -out alt-ca-cert.pem -subj "/CN=Untrusted Root" -nodes

openssl req -new -newkey rsa:2048 -keyout alt-client-key.pem -out alt-client.csr -subj "/CN=bad" -nodes

openssl x509 -req -in alt-client.csr -CA alt-ca-cert.pem -CAkey alt-ca-key.pem -CAcreateserial -out alt-client-cert.pem -days 36500 \
  -extfile <(printf "extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature")
```

Expired private key and cert:
```sh
# many openssl versions do not allow directly creating an expired cert
# through the x509 subcommand, so we use the faketime (libfaketime) tool
# to override the date.
faketime '2008-12-24 08:15:42' openssl req \
  -x509 \
  -newkey rsa:2048 \
  -keyout expired-key.pem \
  -out expired-cert.pem \
  -sha256 \
  -subj "/CN=Expired Test Cert" \
  -days 10 \
  -nodes
```
