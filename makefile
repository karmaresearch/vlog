# Init variables
CUSTOM_LIB =
OUTPUTDIR = .

include makefile.platform

CPPFLAGS = -Iexternal/include -Iinclude -c -MD -MF $(patsubst %.o,%.d,$@) -std=c++11

SRCDIR=src
EXTDIR=external

SRC_FILES = \
	    $(wildcard $(SRCDIR)/*.cpp) \
	    $(wildcard $(SRCDIR)/**/*.cpp) \
	    $(wildcard $(SRCDIR)/**/**/*.cpp)
# Compile also all files in the external directory using the same compiler flags so far
EXT_FILES = \
	    $(wildcard $(EXTDIR)/**/*.cpp) \
	    $(wildcard $(EXTDIR)/**/**/*.cpp) \
	    $(wildcard $(EXTDIR)/**/**/**/*.cpp) \
	    $(wildcard $(EXTDIR)/**/**/**/**/*.cpp)

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
