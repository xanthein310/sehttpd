CFLAGS = -std=gnu11 -Wall -Wextra -Werror
LDFLAGS = -lpthread

all: htstress

htstress: htstress.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	$(RM) htstress
