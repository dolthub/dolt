# standard NR-make boilerplate, to be included at the end of a file
d := $(dirstack_$(sp))
sp := $(basename $(sp))
