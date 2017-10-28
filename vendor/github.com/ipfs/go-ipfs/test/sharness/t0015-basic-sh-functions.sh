#!/bin/sh
#
# Copyright (c) 2015 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test some basic shell functions"

. lib/test-lib.sh

test_expect_success "shellquote works with simple stuff" '
  var=$(shellquote one two)
'

test_expect_success "shellquote output looks good" '
  test "$var" = "'\''one'\'' '\''two'\''" ||
  test_fsh echo "var is \"$var\" instead of \"'\''one'\'' '\''two'\''\""
'

# The following two printf statements are equivalent:
# printf "%s\n" \''"foo\
# bar'
# printf "\047\042\146\157\157\134\012\142\141\162\012"
# We use the second one to simplify quoting.

test_expect_success "shellquote works with complex printf" '
  eval "$(shellquote printf "\047\042\146\157\157\134\012\142\141\162\012")" >actual
'

test_expect_success "shellquote output looks good" '
  printf "\047\042\146\157\157\134\012\142\141\162\012" >expected &&
  test_cmp expected actual
'

test_expect_success "shellquote works with many different bytes" '
  bytes_sans_NUL=$(
    printf "\001\002\003\004\005\006\007\010\011\013\014\015\016\017\020\021\022\023\024\025\026\027\030\031\032\033\034\035\036\037\040\041\042\043\044%%\046\047\050\051\052\053\054\055\056\057\060\061\062\063\064\065\066\067\070\071\072\073\074\075\076\077\100\101\102\103\104\105\106\107\110\111\112\113\114\115\116\117\120\121\122\123\124\125\126\127\130\131\132\133\134\135\136\137\140\141\142\143\144\145\146\147\150\151\152\153\154\155\156\157\160\161\162\163\164\165\166\167\170\171\172\173\174\175\176\177\200\201\202\203\204\205\206\207\210\211\212\213\214\215\216\217\220\221\222\223\224\225\226\227\230\231\232\233\234\235\236\237\240\241\242\243\244\245\246\247\250\251\252\253\254\255\256\257\260\261\262\263\264\265\266\267\270\271\272\273\274\275\276\277\300\301\302\303\304\305\306\307\310\311\312\313\314\315\316\317\320\321\322\323\324\325\326\327\330\331\332\333\334\335\336\337\340\341\342\343\344\345\346\347\350\351\352\353\354\355\356\357\360\361\362\363\364\365\366\367\370\371\372\373\374\375\376\377"
  ) &&
  eval "$(shellquote printf "%s" "$bytes_sans_NUL")" >actual
'

test_expect_success "shellquote output looks good" '
  printf "%s" "$bytes_sans_NUL" >expected &&
  test_cmp expected actual
'

test_done
