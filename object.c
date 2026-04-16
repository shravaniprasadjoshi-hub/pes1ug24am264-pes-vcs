// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "object.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <linux/limits.h>

// ... (Keep the PROVIDED functions: hash_to_hex, hex_to_hash, compute_hash, etc. exactly as they were) ...

int object_write(ObjectType type, const void *data, size_t size, ObjectID *id) {
    // 1. Prepare the Header
    char header[64];
    const char *type_str = (type == OBJ_BLOB) ? "blob" : 
                           (type == OBJ_TREE) ? "tree" : "commit";
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, size) + 1; 

    // 2. Build the full content for hashing
    size_t total_len = header_len + size;
    unsigned char *full_content = malloc(total_len);
    if (!full_content) return -1; 

    memcpy(full_content, header, header_len);
    memcpy(full_content + header_len, data, size);

    // 3. Compute the SHA-256 hash
    compute_hash(full_content, total_len, id);

    // 4. Deduplication: success if it already exists
    if (object_exists(id)) {
        free(full_content);
        return 0;
    }

    // 5. Setup Paths
    char path[PATH_MAX];
    object_path(id, path, sizeof(path));

    char dir_path[PATH_MAX];
    strncpy(dir_path, path, PATH_MAX);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) *last_slash = '\0';

    // 6. Create directories (sharding)
    mkdir(".pes", 0755);
    mkdir(".pes/objects", 0755);
    mkdir(dir_path, 0755);

    // 7. Atomic Write
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_content);
        return -1;
    }

    if (write(fd, full_content, total_len) != (ssize_t)total_len) {
        close(fd);
        free(full_content);
        return -1;
    }

    fsync(fd);
    close(fd);
    free(full_content);

    return 0; 
}

// ... (Leave object_read as a TODO for now) ...

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out

// HINTS - Useful syscalls and functions for this phase:
//   - sprintf / snprintf : formatting the header string
//   - compute_hash       : hashing the combined header + data
//   - object_exists      : checking for deduplication
//   - mkdir              : creating the shard directory (use mode 0755)
//   - open, write, close : creating and writing to the temp file
//                          (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
//   - fsync              : flushing the file descriptor to disk
//   - rename             : atomically moving the temp file to the final path
//

//
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

    // 4. Deduplication: If object exists, we are done!
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

    // 6. Create Directories
    mkdir(".pes", 0755);
    mkdir(".pes/objects", 0755);
    mkdir(dir_path, 0755);

    // 7. Atomic Write using the FULL content
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_content);
        return -1;
    }

    if (write(fd, full_content, total_len) != (ssize_t)total_len) {
        close(fd);
        free(full_content);
        return -1;
    }

    fsync(fd);
    close(fd);
    free(full_content); // Free only AFTER the write is done

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
    // TODO: Implement
    (void)id; (void)type_out; (void)data_out; (void)len_out;
    return -1;
}
