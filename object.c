// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "include/object.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/limits.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[PATH_MAX];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── PHASE 1: IMPLEMENTATION ────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t size, ObjectID *id) {
    // 1. Prepare the Header
    char header[64];
    const char *type_str = (type == OBJ_BLOB) ? "blob" : 
                           (type == OBJ_TREE) ? "tree" : "commit";
    
    // Format: "type size\0"
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, size) + 1; 

    // 2. Build the full object for hashing
    size_t total_len = header_len + size;
    unsigned char *full_content = malloc(total_len);
    if (!full_content) return -1; 

    memcpy(full_content, header, header_len);
    memcpy(full_content + header_len, data, size);

    // 3. Compute Hash
    compute_hash(full_content, total_len, id);

    // 4. Deduplication: If object exists, return success
    if (object_exists(id)) {
        free(full_content);
        return 0;
    }

    // 5. Prepare Paths
    char path[PATH_MAX];
    object_path(id, path, sizeof(path));

    char dir_path[PATH_MAX];
    strncpy(dir_path, path, PATH_MAX);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) *last_slash = '\0';

    // 6. Create Directories (Shard directory XX)
    mkdir(".pes", 0755);
    mkdir(".pes/objects", 0755);
    mkdir(dir_path, 0755);

    // 7. Atomic Write: Write to a temp file then rename
    char temp_path[PATH_MAX];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);

    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_content);
        return -1;
    }

    if (write(fd, full_content, total_len) != (ssize_t)total_len) {
        close(fd);
        unlink(temp_path);
        free(full_content);
        return -1;
    }

    fsync(fd);
    close(fd);

    // Atomic move
    if (rename(temp_path, path) != 0) {
        unlink(temp_path);
        free(full_content);
        return -1;
    }

    free(full_content);
    return 0; 
}

// Read an object from the store.
//
// Steps:
//   1. Build the file path from the hash using object_path()
//   2. Open and read the entire file
//   3. Parse the header to extract the type string and size
//   4. Verify integrity: recompute the SHA-256 of the file contents
//      and compare to the expected hash (from *id). Return -1 if mismatch.
//   5. Set *type_out to the parsed ObjectType
//   6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// HINTS - Useful syscalls and functions for this phase:
//   - object_path        : getting the target file path
//   - fopen, fread, fseek: reading the file into memory
//   - memchr             : safely finding the '\0' separating header and data
//   - strncmp            : parsing the type string ("blob", "tree", "commit")
//   - compute_hash       : re-hashing the read data for integrity verification
//   - memcmp             : comparing the computed hash against the requested hash
//   - malloc, memcpy     : allocating and returning the extracted data
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[PATH_MAX];
    object_path(id, path, sizeof(path)); //

    FILE *fp = fopen(path, "rb"); // Open in binary mode
    if (!fp) return -1;
    
    // TODO: Phase 2.2 - Reading logic goes here
    fclose(fp);
    return -1; 
}
