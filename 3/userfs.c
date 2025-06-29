#include "userfs.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

enum {
    BLOCK_SIZE = 512,
    MAX_FILE_SIZE = 1024 * 1024 * 100,
};

static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
    char *memory;
    int occupied;
    struct block *next;
    struct block *prev;
};

struct file {
    struct block *block_list;
    struct block *last_block;
    int refs;
    char *name;
    struct file *next;
    struct file *prev;
    size_t size;
    bool deleted;
};

static struct file *file_list = NULL;

struct filedesc {
    struct file *file;
    size_t pos;
    struct block *cur_block;
    int flags;
};

static struct filedesc **file_descriptors = NULL;
static int file_descriptor_capacity = 0;

enum ufs_error_code ufs_errno() {
    return ufs_error_code;
}

static struct file *find_file(const char *filename) {
    for (struct file *f = file_list; f != NULL; f = f->next) {
        if (!f->deleted && strcmp(f->name, filename) == 0) {
            return f;
        }
    }
    return NULL;
}

static void free_file(struct file *file) {
    struct block *blk = file->block_list;
    while (blk != NULL) {
        struct block *next = blk->next;
        free(blk->memory);
        free(blk);
        blk = next;
    }
    free(file->name);
    free(file);
}

static void remove_file_from_list(struct file *file) {
    if (file->prev != NULL) {
        file->prev->next = file->next;
    } else {
        file_list = file->next;
    }
    if (file->next != NULL) {
        file->next->prev = file->prev;
    }
}

static int allocate_fd(struct filedesc *fdesc) {
    for (int i = 0; i < file_descriptor_capacity; i++) {
        if (file_descriptors[i] == NULL) {
            file_descriptors[i] = fdesc;
            return i;
        }
    }

    int new_capacity = file_descriptor_capacity == 0 ? 16 : file_descriptor_capacity * 2;
    struct filedesc **new_array = realloc(file_descriptors, new_capacity * sizeof(struct filedesc *));
    if (new_array == NULL) {
        return -1;
    }
    for (int i = file_descriptor_capacity; i < new_capacity; i++) {
        new_array[i] = NULL;
    }
    file_descriptors = new_array;
    int fd = file_descriptor_capacity;
    file_descriptors[fd] = fdesc;
    file_descriptor_capacity = new_capacity;
    return fd;
}

int ufs_open(const char *filename, int flags) {
    struct file *file = find_file(filename);
    bool created = false;

    if (file == NULL) {
        if (!(flags & UFS_CREATE)) {
            ufs_error_code = UFS_ERR_NO_FILE;
            return -1;
        }
        file = malloc(sizeof(struct file));
        if (file == NULL) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
        file->name = strdup(filename);
        if (file->name == NULL) {
            free(file);
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
        file->block_list = NULL;
        file->last_block = NULL;
        file->refs = 0;
        file->size = 0;
        file->deleted = false;
        file->next = file_list;
        file->prev = NULL;
        if (file_list != NULL) {
            file_list->prev = file;
        }
        file_list = file;
        created = true;
    }

    struct filedesc *fdesc = malloc(sizeof(struct filedesc));
    if (fdesc == NULL) {
        if (created) {
            remove_file_from_list(file);
            free_file(file);
        }
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    fdesc->file = file;
    fdesc->pos = 0;
    fdesc->cur_block = file->block_list;
    fdesc->flags = flags;
    file->refs++;

    int fd = allocate_fd(fdesc);
    if (fd == -1) {
        file->refs--;
        if (created) {
            remove_file_from_list(file);
            free_file(file);
        }
        free(fdesc);
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    return fd;
}

ssize_t ufs_write(int fd, const char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *fdesc = file_descriptors[fd];
    struct file *file = fdesc->file;

    if (fdesc->pos + size > MAX_FILE_SIZE) {
        size = MAX_FILE_SIZE - fdesc->pos;
        if (size == 0) {
            ufs_error_code = UFS_ERR_NO_MEM;
            return -1;
        }
    }

    size_t written = 0;
    size_t current_pos = fdesc->pos;

    while (written < size) {
        size_t block_index = current_pos / BLOCK_SIZE;
        size_t offset_in_block = current_pos % BLOCK_SIZE;
        size_t to_write = BLOCK_SIZE - offset_in_block;
        if (to_write > size - written) {
            to_write = size - written;
        }

        struct block *blk = file->block_list;
        for (size_t i = 0; i < block_index; i++) {
            if (blk == NULL) {
                ufs_error_code = UFS_ERR_NO_MEM;
                return -1;
            }
            blk = blk->next;
        }

        if (blk == NULL) {
            struct block *new_block = malloc(sizeof(struct block));
            if (new_block == NULL) {
                ufs_error_code = UFS_ERR_NO_MEM;
                return written > 0 ? written : -1;
            }
            new_block->memory = malloc(BLOCK_SIZE);
            if (new_block->memory == NULL) {
                free(new_block);
                ufs_error_code = UFS_ERR_NO_MEM;
                return written > 0 ? written : -1;
            }
            new_block->occupied = 0;
            new_block->next = NULL;
            new_block->prev = file->last_block;

            if (file->last_block != NULL) {
                file->last_block->next = new_block;
            } else {
                file->block_list = new_block;
            }
            file->last_block = new_block;
            blk = new_block;
        }

        memcpy(blk->memory + offset_in_block, buf + written, to_write);
        if (offset_in_block + to_write > (size_t)blk->occupied) {
            blk->occupied = offset_in_block + to_write;
        }

        written += to_write;
        current_pos += to_write;

        if (fdesc->cur_block == blk) {
            fdesc->pos = current_pos;
        }
    }

    fdesc->pos = current_pos;
    if (fdesc->pos > file->size) {
        file->size = fdesc->pos;
    }

    if (fdesc->cur_block == NULL) {
        fdesc->cur_block = file->block_list;
    }

    return written;
}

ssize_t ufs_read(int fd, char *buf, size_t size) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *fdesc = file_descriptors[fd];
    struct file *file = fdesc->file;

    if (fdesc->pos >= file->size) {
        return 0;
    }

    size_t to_read = size;
    if (fdesc->pos + to_read > file->size) {
        to_read = file->size - fdesc->pos;
    }

    size_t read = 0;
    size_t current_pos = fdesc->pos;

    while (read < to_read) {
        size_t block_index = current_pos / BLOCK_SIZE;
        size_t offset_in_block = current_pos % BLOCK_SIZE;
        size_t to_read_in_block = BLOCK_SIZE - offset_in_block;
        if (to_read_in_block > to_read - read) {
            to_read_in_block = to_read - read;
        }

        struct block *blk = file->block_list;
        for (size_t i = 0; i < block_index; i++) {
            if (blk == NULL) {
                return read;
            }
            blk = blk->next;
        }

        if (blk == NULL) {
            break;
        }

        if (offset_in_block >= (size_t)blk->occupied) {
            break;
        }

        size_t available = blk->occupied - offset_in_block;
        if (to_read_in_block > available) {
            to_read_in_block = available;
        }

        memcpy(buf + read, blk->memory + offset_in_block, to_read_in_block);
        read += to_read_in_block;
        current_pos += to_read_in_block;
    }

    fdesc->pos = current_pos;
    if (fdesc->cur_block == NULL) {
        fdesc->cur_block = file->block_list;
    }

    return read;
}

int ufs_close(int fd) {
    if (fd < 0 || fd >= file_descriptor_capacity || file_descriptors[fd] == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    struct filedesc *fdesc = file_descriptors[fd];
    struct file *file = fdesc->file;

    file->refs--;
    if (file->refs == 0 && file->deleted) {
        remove_file_from_list(file);
        free_file(file);
    }

    free(fdesc);
    file_descriptors[fd] = NULL;

    return 0;
}

int ufs_delete(const char *filename) {
    struct file *file = find_file(filename);
    if (file == NULL) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    if (file->refs == 0) {
        remove_file_from_list(file);
        free_file(file);
    } else {
        file->deleted = true;
    }

    return 0;
}


#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
    return 0;
}
#endif



void ufs_destroy(void) {
    struct file *file = file_list;
    while (file != NULL) {
        struct file *next = file->next;
        free_file(file);
        file = next;
    }
    file_list = NULL;

    for (int i = 0; i < file_descriptor_capacity; i++) {
        if (file_descriptors[i] != NULL) {
            free(file_descriptors[i]);
        }
    }
    free(file_descriptors);
    file_descriptors = NULL;
    file_descriptor_capacity = 0;
}