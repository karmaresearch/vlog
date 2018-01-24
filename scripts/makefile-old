# Init variables
OUTPUTDIR = .
# To enable MYSQL we must add the parameter MYSQL=1
# To enable ODBC we must add the parameter ODBC=1
# To enable MAPI we must add the parameter MAPI=1

# where is trident? Default below, but can be overriden on the commandline.
ifndef $(KOGNAC)
 KOGNAC = ../trident
endif

ifndef $(TRIDENT)
 TRIDENT = ../trident
endif


RDF3X = $(TRIDENT)/rdf3x
CPLUS = g++

#Add dependencies. We compile trident, kognac, and RDF3X
CPPFLAGS=-Iinclude
CPPFLAGS+= -I$(TRIDENT)/include
CPPFLAGS+= -I$(TRIDENT)/include/layers
CPPFLAGS+= -I$(RDF3X)/include
CPPFLAGS+= -I$(KOGNAC)/include
CPPFLAGS+= -isystem /usr/local/include

#Other flags
CPPFLAGS += -c -MD -MF $(patsubst %.o,%.d,$@) -std=c++1z
CPPFLAGS += -DUSE_COMPRESSED_COLUMNS -DPRUNING_QSQR=1 

SRCDIR=src

SRC_FILES = \
			$(wildcard $(SRCDIR)/vlog/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/common/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/backward/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/forward/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/magic/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/web/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/inmemory/*.cpp) \
			$(wildcard $(SRCDIR)/vlog/trident/*.cpp)

#Add also the launcher with the main() file. This file depends on RDF3X (for querying)
SRC_FILES+= $(wildcard $(SRCDIR)/launcher/*.cpp)

ifeq ($(WEBINTERFACE),1)
	CPPFLAGS +=-DWEBINTERFACE=1	
endif

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

ifeq ($(MDLITE),1)
	MDLITE_ROOT= #To be fixed
	SRC_FILES+=$(wildcard $(SRCDIR)/vlog/mdlite/*.cpp)
	CPPFLAGS+=-I$(MDLITE_ROOT)/src/embedded -DMDLITE
endif

RELEASEFLAGS = -O3 -DNDEBUG=1 -gdwarf-2
DEBUGFLAGS = -O0 -g -gdwarf-2 -Wall -Wno-sign-compare -DDEBUG=1

ifeq ($(DEBUG),1)
	CPPFLAGS+=$(DEBUGFLAGS)
	VLOG=$(PRGNAME_DEBUG)
	BUILDDIR=$(BUILDDIR_DEBUG)
	KOGNACLIB=$(KOGNAC)/build_debug
	TRIDENTLIB=$(TRIDENT)/build_debug
else
	CPPFLAGS+=$(RELEASEFLAGS)
	VLOG=$(PRGNAME_RELEASE)
	BUILDDIR=$(BUILDDIR_RELEASE)
	KOGNACLIB=$(KOGNAC)/build
	TRIDENTLIB=$(TRIDENT)/build
endif

#Set up libs and other linking flags
CLIBS=-Wl,-rpath,$(KOGNACLIB) -L$(KOGNACLIB) -Wl,-rpath,$(TRIDENTLIB) -L$(TRIDENTLIB)
LDFLAGS= -lkognac-log -lkognac -ltrident-core -ltrident-sparql -lz -ltbb -lpthread

ifeq ($(WEBINTERFACE),1)
	LDFLAGS+=-lcurl -lboost_system
endif

uname_S := $(shell uname -s)
#ifeq ($(uname_S), Darwin)
#endif
ifeq ($(uname_S), Linux)
	LDFLAGS += -fopenmp
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
ifeq ($(MAPI),1)
	LDFLAGS += -lmapi
endif

#MDLite integration
ifeq ($(MDLITE),1)
	LDFLAGS += -lmonetdb5
endif

OFILES = \
		 $(subst $(SRCDIR),$(BUILDDIR),$(SRC_FILES:.cpp=.o))

PRGNAME_RELEASE=$(OUTPUTDIR)/vlog
BUILDDIR_RELEASE=$(OUTPUTDIR)/build
BUILDDIR_DEBUG=$(OUTPUTDIR)/build_debug
PRGNAME_DEBUG=$(OUTPUTDIR)/vlog_debug

$(VLOG): init $(OFILES)
	$(CPLUS) -o $@ $(CLIBS) $(OFILES) $(LDFLAGS)

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

.PHONY: clean
clean:	oclean
	@rm -rf $(PRGNAME_RELEASE)
	@rm -rf $(PRGNAME_DEBUG)
	@echo "Cleaning completed"
