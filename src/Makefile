CXX       = gcc
CXXFLAGS  = -Wall -I. -Winline -O3 #-fopenmp
LDFLAGS =

PROGRAM   = chunkier
SRCS      = main.o
OBJS      = $(SRCS:.c=.o)

.SUFFIXES: .c .o

.c.o:
	$(CXX) -c $(CXXFLAGS) $< -o $@

all: $(PROGRAM)

run: all
	./$(PROGRAM)

$(PROGRAM): $(OBJS)
	$(CXX) -o $(PROGRAM) $(SRCS) $(CXXFLAGS) $(LDFLAGS)

clean:
	/bin/rm -f $(OBJS) $(PROGRAM) *~
