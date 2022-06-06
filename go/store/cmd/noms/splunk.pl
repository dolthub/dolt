#!/usr/bin/perl

my $noms_dir = '.dolt/noms';

if (! -d $noms_dir) {
    die "Use splunk in a dolt directory";
}

if (! check_exists_command('noms')) {
    die "noms binary not found on $PATH";
}   

print "Welcome to the splunk shell for exploring dolt repository storage.\n";

my $manifest = `noms manifest $noms_dir`;
my $root = get_root($manifest);

my $message = "Currently examining root.\nUse numeric labels to navigate the tree\n.. to back up a level, / to return to root.\nType quit or exit to exit.\n";

my $hash = $root;
my @stack = ($root);

while (true) {
    my $labels = print_show($hash);

    print $message if $message;
    $message = "";
    
    print "> ";
    my $input = <>;
    chomp $input;
    if ($input eq "quit" or $input eq "exit") {
        print "Bye\n";
        exit 0;
    }

    if ($input eq "..") {
        if (scalar @stack <= 1) {
            $message = "At top level, cannot go back\n";
            next;
        }

        shift @stack;
        $hash = $stack[0];
        next;
    }

    if ($input eq "/") {
        $hash = $root;
        @stack = ($root);
        next;
    }

    if (not defined $labels->{$input}) {
        $message = "Invalid selection\nChoose a numeric label from the output above, or:\n.. to go back a level, / to go to the root\n";
        next;
    }

    $hash = $labels->{$input};
    unshift @stack, $hash;
}

sub show {
    my $hash = shift;
    my $cmd = "noms show $noms_dir\:\:#$hash";
    return `$cmd`;
}

sub get_root {
    my $manifest = shift;
    for my $line (split /\n/, $manifest) {
        next unless $line =~ /root/;
        $line =~ /root:\s+([a-z0-9]{32})/;
        return $1;
    }

    die "couldn't determine root in $manifest";
}

sub print_show {
    my $hash = shift;

    my %hashes;
    my $label = 1;
    
    my $noms_show_output = show($hash);
    for my $line (split /\n/, $noms_show_output) {
        if ($line =~ /#([a-z0-9]{32})/ ) {
            $h = $1;
            if ( $1 =~ /[a-z1-9]/ ) {
                $hashes{$label} = $h;
                print "$label)   $line\n";
                $label++;
            } else {
                print "     $line\n";
            }
        } else {
            print "     $line\n";
        }
    }

    return \%hashes;
}

sub check_exists_command { 
    my $check = `sh -c 'command -v $_[0]'`; 
    return $check;
}
