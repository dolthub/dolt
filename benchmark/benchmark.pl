#!/usr/bin/perl -w

use strict;

# Set up the environment
# TO DO: Figure out a portable way to get dolt in the path
$ENV{'PATH'} = $ENV{'PATH'} . ':~/go/bin/';
$ENV{'NOMS_VERSION_NEXT'} = 1;

# Define the benchmarks we will run.
my $benchmarks = [
    {
     'name' => 'git vs dolt raw',
     'prep' => [],
     'git'  => 'git',
     'dolt' => 'dolt',
     'cleanup' => [],
    },
    {
     'name' => 'git init vs dolt init',
     'prep' => 
	 [
	  'mkdir /var/tmp/git-benchmark',
	 ],
      'git' => 'git init /var/tmp/git-benchmark',
      'dolt' => 'dolt init',
      'cleanup' => 
         [
	  'rm -rf .dolt',
	  'rm -rf /var/tmp/git-benchmark'
	 ],
    },
];

# Run the benchmarks
foreach my $benchmark ( @{$benchmarks} ) {
    print $benchmark->{'name'} . "\n";

    foreach my $prep ( @{$benchmark->{'prep'}} ) {
	`$prep`;
	if ($?) {
	    die "Error running: $prep\n";
	}
    }
 
    my ($git_r, $git_u, $git_s)    = 
	time_command($benchmark->{'git'});
    my ($dolt_r, $dolt_u, $dolt_s) = 
	time_command($benchmark->{'dolt'});

    print "Git: $git_r" . "ms\nDolt: $dolt_r" . "ms\n";

    foreach my $cleanup ( @{$benchmark->{'cleanup'}} ) {
	`$cleanup`;
	if ($?) {
	    die "Error running: $cleanup\n";
	}
    }
}

###################################################################################
#
# Functions
#
###################################################################################

sub time_command {
    my $command = shift;

    # time outputs to STDERR so I'll trash STDOUT and grab STDERR from
    # STDOUT which `` writes to
    my $piped_command = "{ time $command ;} 2>&1 1>/dev/null";

    my $output = `$piped_command`;
    # To Do: Some of these commands expect to exit 1. ie, git and dolt.
    # I need to build in an expect into the benchmark definition
    # if ($?) {
    #     die "Error running: $piped_command\n";
    # }

    $output =~ /real\s+(.+)\nuser\s+(.+)\nsys\s+(.+)\n/;

    my $real   = convert_time_output_to_ms($1);
    my $user   = convert_time_output_to_ms($2);
    my $system = convert_time_output_to_ms($3);

    return ($real, $user, $system);
}

sub convert_time_output_to_ms {
    my $time = shift;

    $time =~ /(\d+)m(\d+)\.(\d+)s/;

    my $minutes = $1;
    my $seconds = $2;
    my $ms      = $3;

    return $ms + ($seconds*1000) + ($minutes*60*1000);
}
