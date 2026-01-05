#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <limits.h>
#include <linux/limits.h>

#define BUFFER_SIZE 16* 1024

typedef struct task {
    char source[PATH_MAX];
    char dst[PATH_MAX];
} task_t;

void *copy_file(void *arg) {
    task_t *task = (task_t *)arg;
    int src_fd = -1;
    int dst_fd = -1;
    struct stat src_stat;
    int err = lstat(task->source, &src_stat);
    if (err == -1) {
        fprintf(stderr, "copy_file: lstat error\n");
        free(task);
        return NULL;
    }
    while (1) {
        src_fd = open(task->source, O_RDONLY);
        if (src_fd != -1) {
            break;
        }
        if (errno != EMFILE) {
            fprintf(stderr, "copy_file: open source error\n");
            free(task);
            return NULL;
        }
        sleep(1);
    }
    while (1) {
        dst_fd = open(task->dst, O_WRONLY | O_CREAT | O_TRUNC, src_stat.st_mode);
        if (dst_fd != -1) {
            break;
        }
        if (errno != EMFILE) {
            fprintf(stderr, "copy_file: open destination error\n");
            close(src_fd);
            free(task);
            return NULL;
        }
        sleep(1);
    }
    ssize_t bytes_read;
    ssize_t bytes_written;
    char buffer[BUFFER_SIZE];
    while ((bytes_read = read(src_fd, buffer, sizeof(buffer))) > 0) {
        ssize_t total_written = 0;
        while (total_written < bytes_read) {
            bytes_written = write(dst_fd, buffer + total_written, bytes_read - total_written);
            if (bytes_written == -1) {
                fprintf(stderr, "copy_file: write error\n");
                close(src_fd);
                close(dst_fd);
                free(task);
                return NULL;
            }
            total_written += bytes_written;
        }
    }
    if (bytes_read == -1) {
        fprintf(stderr, "copy_file: read error\n");
    }
    close(src_fd);
    close(dst_fd);
    free(task);
    return NULL;
}

int safe_pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void *), void *arg) {
    int err;
    while (1) {
        err = pthread_create(thread, attr, start_routine, arg);
        if (err == 0) {
            break;
        }
        if (err != EAGAIN) {
            break;
        }
        sleep(1);
    }
    return err;
}

int build_safe_path(const char *dir, const char *file, char *out, size_t out_size) {
    size_t dir_len = strlen(dir);
    size_t file_len = strlen(file);
    if (dir_len + file_len + 2 > out_size) {
        fprintf(stderr, "build_safe_path: resulting path too long: %s/%s\n", dir, file);
        return -1;
    }
    snprintf(out, out_size, "%s/%s", dir, file);
    return 0;
}

void *process_directory(void *arg) {
    task_t *task = (task_t *)arg;
    DIR *dir = NULL;
    struct stat src_stat;
    int err = lstat(task->source, &src_stat);
    if (err == -1) {
        fprintf(stderr, "process_directory: lstat error\n");
        free(task);
        return NULL;
    }
    err = mkdir(task->dst, src_stat.st_mode);
    if (err == -1 && errno != EEXIST) {
        fprintf(stderr, "process_directory: mkdir error\n");
        free(task);
        return NULL;
    }
    long name_max = pathconf(task->source, _PC_NAME_MAX);
    if (name_max == -1) {
        name_max = 255;
    }
    size_t readdir_r_buf_size = sizeof(struct dirent) + name_max + 1;
    struct dirent *readdir_r_buf = malloc(readdir_r_buf_size);
    if (!readdir_r_buf) {
        fprintf(stderr, "process_directory: malloc error\n");
        free(task);
        return NULL;
    }
    while (1) {
        dir = opendir(task->source);
        if (dir != NULL) {
            break;
        }
        if (errno != EMFILE) {
            fprintf(stderr, "process_directory: opendir error\n");
            free(readdir_r_buf);
            free(task);
            return NULL;
        }
        sleep(1);
    }
    int file_count = 0;
    struct dirent *entry = NULL;
    while (readdir_r(dir, readdir_r_buf, &entry) == 0 && entry != NULL) {
        if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) {
            file_count++;
        }
    }
    pthread_t *threads = malloc(sizeof(*threads) * file_count);
    if (!threads && file_count > 0) {
        fprintf(stderr, "process_directory: malloc threads error\n");
        closedir(dir);
        free(readdir_r_buf);
        free(task);
        return NULL;
    }
    int thread_count = 0;
    rewinddir(dir);
    while (readdir_r(dir, readdir_r_buf, &entry) == 0 && entry != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char src_full[PATH_MAX];
        char dst_full[PATH_MAX];
        if (build_safe_path(task->source, entry->d_name, src_full, PATH_MAX) != 0) {
            continue;
        }
        if (build_safe_path(task->dst, entry->d_name, dst_full, PATH_MAX) != 0) {
            continue;
        }
        struct stat st;
        if (lstat(src_full, &st) == -1) {
            fprintf(stderr, "process_directory: lstat error\n");
            continue;
        }
        int create_err = -1;
        task_t *new_task = malloc(sizeof(*new_task));
        if (!new_task) {
            fprintf(stderr, "process_directory: malloc error\n");
            continue;
        }
        strncpy(new_task->source, src_full, PATH_MAX);
        strncpy(new_task->dst, dst_full, PATH_MAX);
        if (S_ISREG(st.st_mode)) {
            create_err = safe_pthread_create(&threads[thread_count], NULL, copy_file, new_task);
        } else if (S_ISDIR(st.st_mode)) {
            create_err = safe_pthread_create(&threads[thread_count], NULL, process_directory, new_task);
        } else {
            free(new_task);
            continue;
        }
        if (create_err == 0) {
            thread_count++;
        } else {
            free(new_task);
        }
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    if (threads) {
        free(threads);
    }
    free(readdir_r_buf);
    closedir(dir);
    free(task);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <source_dir> <dest_dir>\n", argv[0]);
        return 1;
    }

    char real_src[PATH_MAX];
    char real_dst[PATH_MAX];
    if (realpath(argv[1], real_src) == NULL) {
        fprintf(stderr, "main: realpath source error\n");
        return 1;
    }
    struct stat st;
    if (lstat(real_src, &st) == -1) {
        fprintf(stderr, "main: stat source error\n");
        return 1;
    }
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Source is not a directory\n");
        return 1;
    }
    if (mkdir(argv[2], st.st_mode) == -1 && errno != EEXIST) {
        fprintf(stderr, "main: mkdir destination error\n");
        return 1;
    }
    if (realpath(argv[2], real_dst) == NULL) {
        fprintf(stderr, "main: realpath destination error\n");
        return 1;
    }
    size_t len = strlen(real_src);
    if (strncmp(real_src, real_dst, len) == 0) {
        if (real_dst[len] == '\0' || real_dst[len] == '/') {
            fprintf(stderr, "Destination is inside source\n");
            return 1;
        }
    }
    task_t *root_task = malloc(sizeof(*root_task));
    strncpy(root_task->source, real_src, PATH_MAX);
    strncpy(root_task->dst, real_dst, PATH_MAX);
    process_directory(root_task);
    return 0;
}
