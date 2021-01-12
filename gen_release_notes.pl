#!/usr/bin/perl

use warnings;
use strict;

use JSON::Parse 'json_file_to_perl';
use Data::Dumper;

# no arg for changes since last release
my $releaseTag = shift @ARGV;

# fetch the most recent releases for dolt
my $tmpDir = "/var/tmp";
my $releasesFile = "/$tmpDir/releases-$$.json";
my $doltPullsFile = "$tmpDir/dolt-prs-$$.json";
my $gmsPullsFile = "$tmpDir/gms-prs-$$.json";

my $doltReleasesUrl = 'https://api.github.com/repos/dolthub/dolt/releases';
my $curlReleases = "curl -H 'Accept: application/vnd.github.v3+json' '$doltReleasesUrl' > $releasesFile";
system($curlReleases) and die $!;

my $releasesJson = json_file_to_perl($releasesFile);

my ($fromTime, $fromHash, $toTime, $toHash, $fromTag, $toTag);
foreach my $release (@$releasesJson) {
    $fromTime = $release->{created_at};
    $fromTag = $release->{tag_name};
    last if $toTime;

    if ((! $releaseTag) || ($releaseTag eq $release->{tag_name})) {
        $toTime = $release->{created_at};
        last unless $releaseTag;
    }
}

die "Couldn't find release" unless $toTime;

print "from $fromTime to $toTime\n";

my $doltPullRequestsUrl = 'https://api.github.com/repos/dolthub/dolt/pulls?state=closed&per_page=100';
my $curlDoltPulls = "curl -H 'Accept: application/vnd.github.v3+json' '$doltPullRequestsUrl' > $doltPullsFile";
system($curlDoltPulls) and die $!;

my $doltPullsJson = json_file_to_perl($doltPullsFile);
my @mergedDoltPrs;
foreach my $pull (@$doltPullsJson) {
    next unless $pull->{merged_at};
    last if $pull->{merged_at} lt $fromTime;
    my %pr = (
        'url' => $pull->{html_url},
        'number' => $pull->{number},
        'title' => $pull->{title},
        'body' => $pull->{body},
    );

    push (@mergedDoltPrs, \%pr) if $pull->{merged_at} gt $toTime;
}



print Dumper @mergedDoltPrs;


