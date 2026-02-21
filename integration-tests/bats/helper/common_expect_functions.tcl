
proc expect_with_defaults {pattern action} {
    expect {
        -re $pattern {
#            puts "Matched pattern: $pattern"
            eval $action
        }
        timeout {
            puts "<<Timeout>>";
            exit 1
        }
        eof {
            puts "<<End of File reached>>";
            exit 1
        }
        failed {
            puts "<<Failed>>";
            exit 1
        }
    }
}

proc expect_with_defaults_2 {patternA patternB action} {
    # First, match patternA
    expect {
        -re $patternA {
            puts "<<Matched expected pattern A: $patternA>>"
            # Now match patternB
            expect {
                -re $patternB {
                    puts "<<Matched expected pattern B: $patternB>>"
                    eval $action
                }
                timeout {
                    puts "<<Timeout waiting for pattern B>>"
                    exit 1
                }
                eof {
                    puts "<<End of File reached while waiting for pattern B>>"
                    exit 1
                }
                failed {
                    puts "<<Failed while waiting for pattern B>>"
                    exit 1
                }
            }
        }
        timeout {
            puts "<<Timeout waiting for pattern A>>"
            exit 1
        }
        eof {
            puts "<<End of File reached while waiting for pattern A>>"
            exit 1
        }
        failed {
            puts "<<Failed while waiting for pattern A>>"
            exit 1
        }
    }
}

proc expect_without_pattern {bad_pattern action} {
    expect {
        -re $bad_pattern {
            puts "ERROR: Found unexpected pattern: $bad_pattern"
            exit 1
        }
        timeout {
            eval $action
        }
        eof {
            eval $action
        }
        failed {
            puts "<<Failed>>"
            exit 1
        }
    }
}

# Expects output_marker to be matched in the spawn output, then expects expected_prompt and evaluates action. The
# procedure exits with an error if a timeout or EOF occurs before the expected patterns are matched.
proc expect_with_defaults_after {output_marker expected_prompt action} {

    proc _fail {msg} {
        puts $msg
        exit 1
    }

    if {$output_marker ne ""} {
        expect {
            -re $output_marker {}
            timeout { _fail "<<Timeout while expecting output marker: $output_marker>>" }
            eof     { _fail "<<EOF while expecting output marker>>" }
        }
    }

    expect {
        -re $expected_prompt { eval $action }
        timeout { _fail "<<Timeout while expecting prompt: $expected_prompt>>" }
        eof     { _fail "<<EOF while expecting prompt>>" }
    }
}
