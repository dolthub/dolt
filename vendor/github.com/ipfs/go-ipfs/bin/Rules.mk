include mk/header.mk

dist_root_$(d)=/ipfs/QmR27Do9gqx9VmuQTEX1UGXETSWYJTQzPzxS5FNUnySCv1

$(d)/gx: $(d)/gx-v0.12.0
$(d)/gx-go: $(d)/gx-go-v1.5.0

TGTS_$(d) := $(d)/gx $(d)/gx-go
DISTCLEAN += $(wildcard $(d)/gx-v*) $(wildcard $(d)/gx-go-v*) $(d)/tmp

PATH := $(realpath $(d)):$(PATH)

$(TGTS_$(d)):
	rm -f $@
	ln -s $(notdir $^) $@

bin/gx-v%:
	@echo "installing gx $(@:bin/gx-%=%)"
	bin/dist_get $(dist_root_bin) gx $@ $(@:bin/gx-%=%)

bin/gx-go-v%:
	@echo "installing gx-go $(@:bin/gx-go-%=%)"
	@bin/dist_get $(dist_root_bin) gx-go $@ $(@:bin/gx-go-%=%)

CLEAN += $(TGTS_$(d))
include mk/footer.mk
