package crypto

import (
	"testing"
)

func TestEncryptionService_EncryptDecrypt(t *testing.T) {
	es := NewEncryptionService("test-passphrase-32-bytes-long!!")

	plaintext := "Hello, World!"
	ciphertext, err := es.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Verify ciphertext is different from plaintext
	if ciphertext == plaintext {
		t.Error("Ciphertext should not equal plaintext")
	}

	// Verify it has the version prefix
	if len(ciphertext) < 5 || ciphertext[:3] != "v2:" {
		t.Error("Ciphertext should have v2: prefix")
	}

	// Decrypt
	decrypted, err := es.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("Decrypt roundtrip failed: got %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptionService_EmptyValues(t *testing.T) {
	es := NewEncryptionService("test-passphrase-32-bytes-long!!")

	// Empty plaintext should return empty ciphertext
	ciphertext, err := es.Encrypt("")
	if err != nil {
		t.Fatalf("Encrypt empty failed: %v", err)
	}
	if ciphertext != "" {
		t.Errorf("Empty encrypt got %q, want empty", ciphertext)
	}

	// Empty ciphertext should return empty plaintext
	plaintext, err := es.Decrypt("")
	if err != nil {
		t.Fatalf("Decrypt empty failed: %v", err)
	}
	if plaintext != "" {
		t.Errorf("Empty decrypt got %q, want empty", plaintext)
	}
}

func TestEncryptionService_EncryptConfigurationValues(t *testing.T) {
	es := NewEncryptionService("test-passphrase-32-bytes-long!!")

	config := map[string]interface{}{
		"host":     "localhost",
		"port":     8080,
		"api_key":  "secret-key-123",
		"password": "super-secret",
	}

	secretFields := []string{"api_key", "password"}

	encrypted, err := es.EncryptConfigurationValues(config, secretFields)
	if err != nil {
		t.Fatalf("EncryptConfigurationValues failed: %v", err)
	}

	// Non-secret fields should be unchanged
	if encrypted["host"] != "localhost" {
		t.Error("host should be unchanged")
	}
	if encrypted["port"] != 8080 {
		t.Error("port should be unchanged")
	}

	// Secret fields should be encrypted (not equal to original)
	if encrypted["api_key"] == "secret-key-123" {
		t.Error("api_key should be encrypted")
	}
	if encrypted["password"] == "super-secret" {
		t.Error("password should be encrypted")
	}

	// Now decrypt
	decrypted, err := es.DecryptConfigurationValues(encrypted, secretFields)
	if err != nil {
		t.Fatalf("DecryptConfigurationValues failed: %v", err)
	}

	// Verify decrypted values match original
	if decrypted["api_key"] != "secret-key-123" {
		t.Errorf("api_key mismatch: got %v", decrypted["api_key"])
	}
	if decrypted["password"] != "super-secret" {
		t.Errorf("password mismatch: got %v", decrypted["password"])
	}
}

func TestEncryptionService_DifferentPassphrases(t *testing.T) {
	es1 := NewEncryptionService("passphrase-one")
	es2 := NewEncryptionService("passphrase-two")

	plaintext := "secret message"

	// Encrypt with es1
	ciphertext, err := es1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Decrypt with es2 should fail (wrong key)
	_, err = es2.Decrypt(ciphertext)
	if err == nil {
		t.Error("Decrypt with wrong key should have failed")
	}
}
