#!/bin/sh

test_description="Test config command"

. lib/test-lib.sh

# we use a function so that we can run it both offline + online
test_config_cmd_set() {

  # flags (like --bool in "ipfs config --bool")
  cfg_flags="" # unset in case.
  test "$#" = 3 && { cfg_flags=$1; shift; }

  cfg_key=$1
  cfg_val=$2
  test_expect_success "ipfs config succeeds" '
    ipfs config $cfg_flags "$cfg_key" "$cfg_val"
  '

  test_expect_success "ipfs config output looks good" '
    echo "$cfg_val" >expected &&
    ipfs config "$cfg_key" >actual &&
    test_cmp expected actual
  '

  # also test our lib function. it should work too.
  cfg_key="Lib.$cfg_key"
  test_expect_success "test_config_set succeeds" '
    test_config_set $cfg_flags "$cfg_key" "$cfg_val"
  '

  test_expect_success "test_config_set value looks good" '
    echo "$cfg_val" >expected &&
    ipfs config "$cfg_key" >actual &&
    test_cmp expected actual
  '
}

# this is a bit brittle. the problem is we need to test
# with something that will be forced to unmarshal as a struct.
# (i.e. just setting 'ipfs config --json foo "[1, 2, 3]"') may
# set it as astring instead of proper json. We leverage the
# unmarshalling that has to happen.
CONFIG_SET_JSON_TEST='{
  "MDNS": {
    "Enabled": true,
    "Interval": 10
  }
}'

test_config_cmd() {
  test_config_cmd_set "beep" "boop"
  test_config_cmd_set "beep1" "boop2"
  test_config_cmd_set "beep1" "boop2"
  test_config_cmd_set "--bool" "beep2" "true"
  test_config_cmd_set "--bool" "beep2" "false"
  test_config_cmd_set "--json" "beep3" "true"
  test_config_cmd_set "--json" "beep3" "false"
  test_config_cmd_set "--json" "Discovery" "$CONFIG_SET_JSON_TEST"
  test_config_cmd_set "--json" "deep-not-defined.prop" "true"
  test_config_cmd_set "--json" "deep-null" "null"
  test_config_cmd_set "--json" "deep-null.prop" "true"

  test_expect_success "'ipfs config show' works" '
    ipfs config show >actual
  '

  test_expect_success "'ipfs config show' output looks good" '
    grep "\"beep\": \"boop\"," actual &&
    grep "\"beep1\": \"boop2\"," actual &&
    grep "\"beep2\": false," actual &&
    grep "\"beep3\": false," actual
  '

  test_expect_success "setup for config replace test" '
    cp "$IPFS_PATH/config" newconfig.json &&
	sed -i"~" -e /PrivKey/d -e s/10GB/11GB/ newconfig.json &&
	sed -i"~" -e '"'"'/PeerID/ {'"'"' -e '"'"' s/,$// '"'"' -e '"'"' } '"'"' newconfig.json
  '

  test_expect_success "run 'ipfs config replace'" '
	ipfs config replace - < newconfig.json
  '

  test_expect_success "check resulting config after 'ipfs config replace'" '
	sed -e /PrivKey/d "$IPFS_PATH/config" > replconfig.json &&
	sed -i"~" -e '"'"'/PeerID/ {'"'"' -e '"'"' s/,$// '"'"' -e '"'"' } '"'"' replconfig.json &&
	test_cmp replconfig.json newconfig.json
  '

  # SECURITY
  # Those tests are here to prevent exposing the PrivKey on the network

  test_expect_success "'ipfs config Identity' fails" '
       test_expect_code 1 ipfs config Identity 2> ident_out
  '

  test_expect_success "output looks good" '
       echo "Error: cannot show or change private key through API" > ident_exp &&
       test_cmp ident_exp ident_out
  '

  test_expect_success "'ipfs config Identity.PrivKey' fails" '
       test_expect_code 1 ipfs config Identity.PrivKey 2> ident_out
  '

  test_expect_success "output looks good" '
       test_cmp ident_exp ident_out
  '

  test_expect_success "lower cased PrivKey" '
       sed -i"~" -e '\''s/PrivKey/privkey/'\'' "$IPFS_PATH/config" &&
       test_expect_code 1 ipfs config Identity.privkey 2> ident_out
  '

  test_expect_success "output looks good" '
       test_cmp ident_exp ident_out
  '

  test_expect_success "fix it back" '
       sed -i"~" -e '\''s/privkey/PrivKey/'\'' "$IPFS_PATH/config"
  '

  test_expect_success "'ipfs config show' doesn't include privkey" '
       ipfs config show > show_config &&
       test_expect_code 1 grep PrivKey show_config
  '

  test_expect_success "'ipfs config replace' injects privkey back" '
       ipfs config replace show_config &&
       grep "\"PrivKey\":" "$IPFS_PATH/config" | grep -e ": \".\+\"" >/dev/null
  '

  test_expect_success "'ipfs config replace' with privkey errors out" '
       cp "$IPFS_PATH/config" real_config &&
       test_expect_code 1 ipfs config replace - < real_config 2> replace_out
  '

  test_expect_success "output looks good" '
       echo "Error: setting private key with API is not supported" > replace_expected
       test_cmp replace_out replace_expected
  '

  test_expect_success "'ipfs config replace' with lower case privkey errors out" '
       cp "$IPFS_PATH/config" real_config &&
       sed -i -e '\''s/PrivKey/privkey/'\'' real_config &&
       test_expect_code 1 ipfs config replace - < real_config 2> replace_out
  '

  test_expect_success "output looks good" '
       echo "Error: setting private key with API is not supported" > replace_expected
       test_cmp replace_out replace_expected
  '
}

test_init_ipfs

# should work offline
test_config_cmd

# should work online
test_launch_ipfs_daemon
test_config_cmd
test_kill_ipfs_daemon


test_done
