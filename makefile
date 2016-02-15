# Init variables
CUSTOM_LIB =
OUTPUTDIR = .
# To enable MYSQL we must add the parameter MYSQL=1
# To enable ODBC we must add the parameter ODBC=1
# To enable MAPI we must add the parameter MAPI=1

include makefile.platform

CPPFLAGS=-Iinclude
CPPFLAGS+= -Iexternal/include

#Other flags
CPPFLAGS += -c -MD -MF $(patsubst %.o,%.d,$@) -std=c++11

SRCDIR=src
EXTDIR=external

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

# Compile also all files in the external directory using the same compiler flags so far
EXT_FILES = \
			$(wildcard $(EXTDIR)/trident/*.cpp) \
			$(wildcard $(EXTDIR)/trident/**/*.cpp) \
			$(wildcard $(EXTDIR)/trident/**/**/*.cpp) \

RELEASEFLAGS = -O3 -DNDEBUG=1
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

OFILES1 = \
		  $(subst $(SRCDIR),$(BUILDDIR),$(SRC_FILES:.cpp=.o))
	OFILES2 = \
			  $(subst $(EXTDIR),$(BUILDDIR)/ext,$(EXT_FILES:.cpp=.o))

PRGNAME_RELEASE=$(OUTPUTDIR)/vlog
BUILDDIR_RELEASE=$(OUTPUTDIR)/build
BUILDDIR_DEBUG=$(OUTPUTDIR)/build_debug
PRGNAME_DEBUG=$(OUTPUTDIR)/vlog_debug

$(VLOG): init $(OFILES1) $(OFILES2)
	$(CPLUS) -o $@ $(CLIBS) $(OFILES1) $(OFILES2) $(CLIBS) $(LDFLAGS) -lpthread $(CUSTOM_LIBS)

init:
	@mkdir -p $(BUILDDIR)	

$(BUILDDIR)/ext/%.o: $(EXTDIR)/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(CINCLUDES) $(CPPFLAGS) $< -o $@ 

# pull in dependency info for *existing* .o files
-include $(OFILES1:.o=.d)

-include $(OFILES2:.o=.d)

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p `dirname $@`
	$(CPLUS) $(CINCLUDES) $(CPPFLAGS) $< -o $@ 

.PHONY: oclean
oclean:
	@rm -rf $(BUILDDIR_RELEASE)
	@rm -rf $(BUILDDIR_DEBUG)

.PHONY: clean
clean:	oclean
	@rm -rf $(PRGNAME_RELEASE)
	@rm -rf $(PRGNAME_DEBUG)
	@echo "Cleaning completed"
