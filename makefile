# Init variables
CUSTOM_LIBS =
OUTPUTDIR = .
# To enable MYSQL we must add the parameter MYSQL=1
# To enable ODBC we must add the parameter ODBC=1
# To enable MAPI we must add the parameter MAPI=1

TRIDENT_LIB=../trident/build

CPLUS = g++
CC = gcc

uname_S := $(shell uname -s)

ifeq ($(uname_S), Darwin)
	# OSX-dependent stuff
	MTSUFF = -mt

CLIBS = -L$(MYTRIDENT)/build \
		-L/usr/local/lib -Wl,-install_name,/usr/local/lib
endif
ifeq ($(uname_S), Linux)
	# Linux-dependent stuff
	MTSUFF = # -mt

CLIBS = \
		-L/usr/local/lib -Wl,-rpath,/usr/local/lib -L$(MYTRIDENT)/build \
		-fopenmp
endif

LDFLAGS= \
		 -ltrident-core -ltrident-sparql \
		 -llz4 -lcurl \
		 -lboost_filesystem -lboost_system -lboost_chrono \
		 -lboost_thread$(MTSUFF) -lboost_log$(MTSUFF) \
		 -lboost_log_setup$(MTSUFF) \
		 -lboost_program_options -lboost_iostreams

# where is trident? Default below, but can be overriden on the commandline.
TRIDENT = ../trident

# use some name that does not match with anything in the trident paths, because otherwise pattern
# replacement fails when creating names for .o files.
MYTRIDENT = MyTridentLink
MYTRIDENT := $(shell sh ./createTridentLink $(TRIDENT))

RDF3X = $(MYTRIDENT)/rdf3x
KOGNAC = $(MYTRIDENT)/build/kognac

#Add dependencies. We compile trident, kognac, and RDF3X
CPPFLAGS=-Iinclude
CPPFLAGS+= -I$(MYTRIDENT)/include
CPPFLAGS+= -I$(MYTRIDENT)/include/layers
CPPFLAGS+= -I$(MYTRIDENT)/rdf3x/include
CPPFLAGS+= -I$(KOGNAC)/include
CPPFLAGS+= -isystem /usr/local/include
#Take sparsehash from trident
CPPFLAGS+= -I$(TRIDENT)/build/kognac/external/sparsehash/src

#Other flags
CPPFLAGS += -c -MD -MF $(patsubst %.o,%.d,$@) -std=c++11

CPPFLAGS += -DUSE_COMPRESSED_COLUMNS -DPRUNING_QSQR=1 -DWEBINTERFACE=1

SRCDIR=src

SRC_FILES = \
			$(wildcard $(SRCDIR)/vlog/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/common/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/backward/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/forward/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/magic/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/web/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/trident/*.cpp)

#Add also the launcher with the main() file. This file depends on RDF3X (for querying)
SRC_FILES+= $(wildcard $(SRCDIR)/launcher/*.cpp)

ifeq ($(MYSQL),1)
	SRC_FILES+=$(wildcard $(SRCDIR)/vlog/mysql/*.cpp)
	CPPFLAGS+=-DMYSQL
endif


ifeq ($(ODBC),1)
	SRC_FILES+=$(wildcard $(SRCDIR)/vlog/odbc/*.cpp)
	CPPFLAGS+=-DODBC
endif

ifeq ($(MAPI),1)
	SRC_FILES+=$(wildcard $(SRCDIR)/vlog/mapi/*.cpp)
	CPPFLAGS+=-DMAPI
endif

SNAP_FILES = \
			 $(MYTRIDENT)/snap/snap-core/Snap.cpp

RELEASEFLAGS = -O3 -DNDEBUG=1 -gdwarf-2
DEBUGFLAGS = -O0 -g -gdwarf-2 -Wall -Wno-sign-compare -DDEBUG=1

CPPFLAGS += -DBOOST_LOG_DYN_LINK
CFLAGS += -DBOOST_LOG_DYN_LINK

ifeq ($(DEBUG),1)
	CPPFLAGS+=$(DEBUGFLAGS)
	CFLAGS+=$(DEBUGFLAGS)
	VLOG=$(PRGNAME_DEBUG)
	BUILDDIR=$(BUILDDIR_DEBUG)
else
	CPPFLAGS+=$(RELEASEFLAGS)
	CFLAGS+=$(RELEASEFLAGS)
	VLOG=$(PRGNAME_RELEASE)
	BUILDDIR=$(BUILDDIR_RELEASE)
endif

#MySQL integration
ifeq ($(MYSQL),1)
	LDFLAGS += -lmysqlcppconn
endif

#ODBC integration
ifeq ($(ODBC),1)
	LDFLAGS += -lodbc
endif

#MAPI integration
ifeq ($(ODBC),1)
	LDFLAGS += -lmapi
endif

#Add link to Intel TBB library
LDFLAGS += -ltbb # -ltbbmalloc_proxy

OFILES = \
		 $(subst $(SRCDIR),$(BUILDDIR),$(SRC_FILES:.cpp=.o)) \
		 $(subst $(MYTRIDENT),$(BUILDDIR)/trident,$(EXT_FILES:.cpp=.o)) \
		 $(SNAP_FILES:.cpp=.o)

PRGNAME_RELEASE=$(OUTPUTDIR)/vlog
BUILDDIR_RELEASE=$(OUTPUTDIR)/build
BUILDDIR_DEBUG=$(OUTPUTDIR)/build_debug
PRGNAME_DEBUG=$(OUTPUTDIR)/vlog_debug

$(VLOG): init $(OFILES)
	$(CPLUS) -o $@ $(CLIBS) $(OFILES) $(CLIBS) $(LDFLAGS) -lpthread $(CUSTOM_LIBS)

$(MYTRIDENT):	$(TRIDENT)
	-ln -s $(TRIDENT) $(MYTRIDENT)
	echo $(MYTRIDENT)

init:
	@mkdir -p $(BUILDDIR)

# pull in dependency info for *existing* .o files
-include $(OFILES:.o=.d)

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(CPPFLAGS) $< -o $@

.PHONY: oclean
oclean:
	@rm -rf $(BUILDDIR_RELEASE)
	@rm -rf $(BUILDDIR_DEBUG)
	@rm -f $(MYTRIDENT)

.PHONY: clean
clean:	oclean
	@rm -rf $(PRGNAME_RELEASE)
	@rm -rf $(PRGNAME_DEBUG)
	@echo "Cleaning completed"
