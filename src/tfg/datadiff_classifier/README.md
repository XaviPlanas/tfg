# Notas Ollama en docker

[[TOC]]

## Motores validados

- gemma2:2b : pérdida de foco de tarea
- qwen3:8b

## **Descargar** el modelo

```bash
sudo docker exec -e OLLAMA_SKIP_VERIFY=true -it ollama ollama pull gemma2:2b
```

Si da errores de timeout , activar VPN (inspección de paquetes):

```bash
pulling manifest

Error: max retries exceeded: Get "<https://dd20bb891979d25aebc8bec07b2b3bbc.r2.cloudflarestorage.com/ollama/docker/registry/v2/blobs/sha256/74/7462734796d67c40ecec2ca98eddf970e171dbb6b370e43fd633ee75b69abe1b/data?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=66040c77ac1b787c3af820529859349a%2F20260411%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20260411T201246Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=55d7d8df0bf624ced63e7b719e0ecc917cce2c9045c47e660885a3fd8485a459>": tls: failed to verify certificate: x509: certificate is not valid for any names, but wanted to match dd20bb891979d25aebc8bec07b2b3bbc.r2.cloudflarestorage.com
```

Comprobación: (en el ejemplo aparece **core1.netops.test** en lugar de cloudfarestorage como nodo que sirve el certificado!!!)

```bash
$ openssl s_client -connect r2.cloudflarestorage.com:443 -servername r2.cloudflarestorage.com
CONNECTED(00000003)
depth=0 C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = **core1.netops.test**, emailAddress = admin@Packetland
verify error:num=18:self-signed certificate
verify return:1
depth=0 C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = core1.netops.test, emailAddress = admin@Packetland
verify return:1
---

Certificate chain
 0 s:C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = core1.netops.test, emailAddress = admin@Packetland
   i:C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = core1.netops.test, emailAddress = admin@Packetland
   a:PKEY: rsaEncryption, 2048 (bit); sigalg: RSA-SHA256
   v:NotBefore: Feb 19 14:46:27 2026 GMT; NotAfter: Sep 18 14:46:27 2124 GMT
---

Server certificate
-----BEGIN CERTIFICATE-----
...
-----END CERTIFICATE-----
subject=C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = core1.netops.test, emailAddress = admin@Packetland
issuer=C = EU, ST = SOME-ST, L = SOME-CITY, O = Widgits Pty Ltd, OU = Packetland, CN = core1.netops.test, emailAddress = admin@Packetland
---
```

## Ejecutar el modelo

Aunque el proceso ollama esté arriba hay realizar un **run** con el modelo que queremos para que el proceso ollama atienda peticiones:

```bash
docker exec -it ollama ollama run gemma2:2b
```
