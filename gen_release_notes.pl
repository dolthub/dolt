#!/usr/bin/perl

use warnings;
use strict;

use open ":std", ":encoding(UTF-8)";

use JSON::Parse 'json_file_to_perl';
use Data::Dumper;

# no arg for changes since last release
my $releaseTag = shift @ARGV;

my $tmpDir = "/var/tmp";
my $releasesFile = "/$tmpDir/releases-$$.json";
my $doltPullsFile = "$tmpDir/dolt-prs-$$.json";
my $doltIssuesFile = "$tmpDir/dolt-issues-$$.json";
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

my $doltIssuesUrl = "https://api.github.com/repos/dolthub/dolt/issues?state=closed&since=$fromTime&perPage=100";
my $curlDoltIssues = "curl -H 'Accept: application/vnd.github.v3+json' '$doltIssuesUrl' > $doltIssuesFile";
system($curlDoltIssues) and die $!;

my $doltIssuesJson = json_file_to_perl($doltIssuesFile);
my @closedIssues;
foreach my $issue (@$doltIssuesJson) {
    next unless $issue->{closed_at};
    last if $issue->{closed_at} lt $fromTime;
    next if $issue->{html_url} =~ m|/pull/|; # the issues API also returns PR results
    my %i = (
        'url' => $issue->{html_url},
        'number' => $issue->{number},
        'title' => $issue->{title},
        'body' => $issue->{body},
    );

    push (@closedIssues, \%i) if $issue->{closed_at} gt $toTime; 
}

print "# Merged PRs:\n\n";
foreach my $pr (@mergedDoltPrs) {
    if ($pr->{body}) {
        print "* [$pr->{number}]($pr->{url}): $pr->{title} ($pr->{body})\n";        
    } else {
        print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";
    }
}

print "\n\n# Closed Issues\n\n";
foreach my $pr (@closedIssues) {
    print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";
}
