include mk/header.mk

$(d)/preload.go: d:=$(d)
$(d)/preload.go: $(d)/preload_list $(d)/preload.sh
	$(d)/preload.sh > $@
	go fmt $@ >/dev/null

DEPS_GO += $(d)/preload.go
	
include mk/footer.mk
