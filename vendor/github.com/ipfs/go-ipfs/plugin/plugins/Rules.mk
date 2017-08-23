include mk/header.mk

$(d)_plugins:=$(d)/git
$(d)_plugins_so:=$(addsuffix .so,$($(d)_plugins))
$(d)_plugins_main:=$(addsuffix /main/main.go,$($(d)_plugins))


$($(d)_plugins_main): d:=$(d)
$($(d)_plugins_main):
	$(d)/gen_main.sh "$(dir $@).." "$(call go-pkg-name,$(dir $@)/..)"
	go fmt $@ >/dev/null

$($(d)_plugins_so): %.so : %/main/main.go
$($(d)_plugins_so): $$(DEPS_GO) ALWAYS
	go build -buildmode=plugin -i -pkgdir "$(GOPATH)/pkg/linux_amd64_dynlink" $(go-flags-with-tags) -o "$@" "$(call go-pkg-name,$(basename $@))/main"
	chmod +x "$@"

CLEAN += $($(d)_plugins_so)

build_plugins: $($(d)_plugins_so)


include mk/footer.mk
