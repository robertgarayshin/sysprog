#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>

static bool is_builtin_cd(const struct expr *e) {
	return e->type == EXPR_TYPE_COMMAND && strcmp(e->cmd.exe, "cd") == 0;
}

static bool is_builtin_exit(const struct expr *e) {
	return e->type == EXPR_TYPE_COMMAND && strcmp(e->cmd.exe, "exit") == 0;
}

static void exec_builtin_cd(const struct expr *e) {
	if (e->cmd.arg_count < 1) {
		fprintf(stderr, "cd: missing operand\n");
		return;
	}
	if (chdir(e->cmd.args[0]) != 0) {
		perror("cd");
	}
}

static bool line_has_pipe(const struct command_line *line) {
	for (const struct expr *e = line->head; e; e = e->next) {
		if (e->type == EXPR_TYPE_PIPE)
			return true;
	}
	return false;
}

static void exec_builtin_exit(const struct command_line *line, const struct expr *e) {
	int code = 0;
	if (e->cmd.arg_count >= 1) {
		char *end;
		code = (int) strtol(e->cmd.args[0], &end, 10);
	}

	if (e->next != NULL || line_has_pipe(line)) {
		return;
	}

	exit(code);
}

static int execute_pipeline(const struct command_line *line);

static int execute_expr_group(const struct command_line *line, struct expr **current, int prev_status, bool *skip_next) {
	struct expr *group_start = *current;

	struct expr *group_end = NULL;
	const struct expr *next_operator = NULL;

	while (*current && (*current)->type != EXPR_TYPE_AND && (*current)->type != EXPR_TYPE_OR) {
		group_end = *current;
		*current = (*current)->next;
	}

	if (*current) {
		next_operator = *current;
		*current = (*current)->next;
	}

	struct expr *saved_next = group_end->next;
	group_end->next = NULL;

	struct command_line group_line = *line;
	group_line.head = group_start;

	int group_status = 0;
	if (!*skip_next) {
		group_status = execute_pipeline(&group_line);
	} else {
		group_status = prev_status;
	}

	group_end->next = saved_next;

	if (next_operator) {
		if (next_operator->type == EXPR_TYPE_AND) {
			*skip_next = (group_status != 0);
		} else if (next_operator->type == EXPR_TYPE_OR) {
			*skip_next = (group_status == 0);
		}
	} else {
		*skip_next = false;
	}

	return group_status;
}

static int execute_command_line(const struct command_line *line) {
	if (line->is_background) {
		const pid_t pid = fork();
		if (pid < 0) {
			perror("fork");
			exit(1);
		}

		if (pid > 0) {
			int status;
			waitpid(pid, &status, 0);
			return 0;
		}

		pid_t pid2 = fork();
		if (pid2 < 0) {
			perror("fork");
			_exit(1);
		}
		if (pid2 > 0) {
			_exit(0);
		}

		struct expr *current = line->head;
		int status = 0;
		bool skip_next = false;

		while (current) {
			status = execute_expr_group((struct command_line *)line, &current, status, &skip_next);
		}
		_exit(status);
	}

	struct expr *current = line->head;
	int status = 0;
	bool skip_next = false;

	while (current) {
		status = execute_expr_group((struct command_line *)line, &current, status, &skip_next);
	}

	return status;
}

static int execute_pipeline(const struct command_line *line) {
    int pipefd[2] = {-1, -1};
    int prev_fd = -1;
    pid_t pids[1024];
    size_t pid_count = 0;
    int last_status = 0;

    const struct expr *e = line->head;

    while (e) {
        if (e->type == EXPR_TYPE_PIPE) {
            e = e->next;
            continue;
        }

        const struct expr *next = e->next;
        bool has_pipe = next && next->type == EXPR_TYPE_PIPE;

        if (has_pipe && pipe(pipefd) < 0) {
            perror("pipe");
            exit(1);
        }

        if (!has_pipe && prev_fd == -1 && is_builtin_cd(e)) {
            exec_builtin_cd(e);
            e = e->next;
            continue;
        }

        if (is_builtin_exit(e)) {
            bool has_any_pipe = false;
            for (const struct expr *t = line->head; t; t = t->next) {
                if (t->type == EXPR_TYPE_PIPE) {
                    has_any_pipe = true;
                    break;
                }
            }
            if (!has_any_pipe) {
                exec_builtin_exit(line, e);
                continue;
            }
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            exit(1);
        }

        if (pid == 0) {
            if (prev_fd != -1) {
                dup2(prev_fd, STDIN_FILENO);
                close(prev_fd);
            }

            if (has_pipe) {
                close(pipefd[0]);
                dup2(pipefd[1], STDOUT_FILENO);
                close(pipefd[1]);
            } else if (line->out_type == OUTPUT_TYPE_FILE_NEW || line->out_type == OUTPUT_TYPE_FILE_APPEND) {
                int flags = O_WRONLY | O_CREAT;
                flags |= (line->out_type == OUTPUT_TYPE_FILE_NEW) ? O_TRUNC : O_APPEND;
                int fd = open(line->out_file, flags, 0666);
                if (fd < 0) {
                    perror("open");
                    exit(1);
                }
                dup2(fd, STDOUT_FILENO);
                close(fd);
            }

            if (is_builtin_cd(e)) {
                fprintf(stderr, "cd: not supported in pipe\n");
                exit(1);
            }

            if (is_builtin_exit(e)) {
                int code = 0;
                if (e->cmd.arg_count >= 1) {
                    char *end;
                    code = (int) strtol(e->cmd.args[0], &end, 10);
                }
                exit(code);
            }

            char **argv = malloc(sizeof(char *) * (e->cmd.arg_count + 2));
            argv[0] = e->cmd.exe;
            for (uint32_t i = 0; i < e->cmd.arg_count; ++i) {
                argv[i + 1] = e->cmd.args[i];
            }
            argv[e->cmd.arg_count + 1] = NULL;

            execvp(argv[0], argv);
            perror("execvp");
            exit(127);
        }

        pids[pid_count++] = pid;

        if (prev_fd != -1) {
            close(prev_fd);
        }
        if (has_pipe) {
            close(pipefd[1]);
            prev_fd = pipefd[0];
        } else {
            prev_fd = -1;
        }

        e = has_pipe ? next->next : e->next;
    }

    for (size_t i = 0; i < pid_count; ++i) {
        int child_status;
        if (waitpid(pids[i], &child_status, 0) > 0) {
            if (i == pid_count - 1) {
                last_status = WIFEXITED(child_status) ? WEXITSTATUS(child_status) : 1;
            }
        }
    }

    return last_status;
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	struct parser *p = parser_new();
	int exit_code = 0;
	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int) err);
				continue;
			}
			exit_code = execute_command_line(line);
			command_line_delete(line);
		}
	}
	parser_delete(p);
	return exit_code;
}