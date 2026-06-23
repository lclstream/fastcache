# Self-signed certificate generation for the cache supervisor (mTLS).
#   make certs        # generate CA, server, and client certs into certs/
#   make clean-certs  # remove the certs/ directory

CERT_DIR = certs

.PHONY: certs ca server client clean-certs

certs: ca server client

ca: $(CERT_DIR)/ca.crt

$(CERT_DIR)/ca.crt:
	mkdir -p $(CERT_DIR)
	openssl req -x509 -newkey rsa:4096 -days 2 -nodes \
		-keyout $(CERT_DIR)/ca.key -out $(CERT_DIR)/ca.crt \
		-subj "/CN=fastcache-ca"
	chmod 400 $(CERT_DIR)/ca.key $(CERT_DIR)/ca.crt

server: $(CERT_DIR)/server.crt

$(CERT_DIR)/server.crt: $(CERT_DIR)/ca.crt
	openssl req -newkey rsa:4096 -nodes \
		-keyout $(CERT_DIR)/server.key -out $(CERT_DIR)/server.csr \
		-subj "/CN=$(shell hostname -f)" \
		-addext "subjectAltName=DNS:$(shell hostname -f)"
	openssl x509 -req -in $(CERT_DIR)/server.csr \
		-CA $(CERT_DIR)/ca.crt -CAkey $(CERT_DIR)/ca.key -CAcreateserial \
		-days 2 -out $(CERT_DIR)/server.crt -copy_extensions copyall
	rm -f $(CERT_DIR)/server.csr
	chmod 400 $(CERT_DIR)/server.key $(CERT_DIR)/server.crt

client: $(CERT_DIR)/client.crt

$(CERT_DIR)/client.crt: $(CERT_DIR)/ca.crt
	openssl req -newkey rsa:4096 -nodes \
		-keyout $(CERT_DIR)/client.key -out $(CERT_DIR)/client.csr \
		-subj "/CN=lclstream-client"
	openssl x509 -req -in $(CERT_DIR)/client.csr \
		-CA $(CERT_DIR)/ca.crt -CAkey $(CERT_DIR)/ca.key -CAcreateserial \
		-days 2 -out $(CERT_DIR)/client.crt
	rm -f $(CERT_DIR)/client.csr
	chmod 400 $(CERT_DIR)/client.key $(CERT_DIR)/client.crt

clean-certs:
	rm -rf $(CERT_DIR)
