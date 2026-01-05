TARGET_1 = lab3_1
SRCS_1 = lab3_1.c

CC = gcc
RM = rm
CFLAGS = -g -Wall -Werror -Wextra -Wno-deprecated-declarations -pthread

all: ${TARGET_1}

${TARGET_1}: ${SRCS_1}
	${CC} ${CFLAGS} ${SRCS_1} -o ${TARGET_1}
clean:
	${RM} -f *.o ${TARGET_1}
