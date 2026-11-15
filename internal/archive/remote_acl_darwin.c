#include <sys/acl.h>
#include <errno.h>
#include <stddef.h>

static int photoscrawl_acl(int fd, int private_only) {
    acl_t acl = acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
    if (acl == NULL) return errno == ENOENT;
    acl_entry_t entry;
    int position = ACL_FIRST_ENTRY;
    int valid = 1;
    while (1) {
        errno = 0;
        if (acl_get_entry(acl, position, &entry) != 0) {
            valid = errno == EINVAL;
            break;
        }
        acl_tag_t tag;
        if (private_only || acl_get_tag_type(entry, &tag) != 0 || tag != ACL_EXTENDED_DENY) {
            valid = 0;
            break;
        }
        position = ACL_NEXT_ENTRY;
    }
    acl_free(acl);
    return valid;
}

int photoscrawl_private_acl(int fd) { return photoscrawl_acl(fd, 1); }
int photoscrawl_ancestor_acl(int fd) { return photoscrawl_acl(fd, 0); }
