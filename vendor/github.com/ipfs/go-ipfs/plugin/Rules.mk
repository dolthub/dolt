include mk/header.mk

dir := $(d)/loader
include $(dir)/Rules.mk

dir := $(d)/plugins
include $(dir)/Rules.mk

include mk/footer.mk
