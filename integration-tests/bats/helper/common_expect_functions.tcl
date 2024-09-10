
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

