#!/usr/bin/perl

use warnings;
use strict;

use open ":std", ":encoding(UTF-8)";

use JSON::Parse 'json_file_to_perl';
use Data::Dumper;

use Getopt::Long;

my $token = "";
GetOptions ("token|t=s" => \$token) or die "Error in command line args";

# no arg for changes since last release
my $releaseTag = shift @ARGV;

print STDERR "Looking for changes since release $releaseTag\n" if $releaseTag;

my $tmpDir = "/var/tmp";
my $curlFile = "$tmpDir/curl-$$.out";

my $doltReleasesUrl = 'https://api.github.com/repos/dolthub/dolt/releases';
my $curlReleases = curlCmd($doltReleasesUrl, $token);

print STDERR "$curlReleases\n";
system($curlReleases) and die $!;

my $releasesJson = json_file_to_perl($curlFile);

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

print STDERR "Looking for PRs and issues from $fromTime to $toTime\n";

my $doltPullRequestsUrl = 'https://api.github.com/repos/dolthub/dolt/pulls';
my $mergedDoltPrs = getPRs($doltPullRequestsUrl, $fromTime, $toTime);

my $doltIssuesUrl = "https://api.github.com/repos/dolthub/dolt/issues";
my $closedIssues = getIssues($doltIssuesUrl, $fromTime, $toTime);

print "# Merged PRs:\n\n";
foreach my $pr (@$mergedDoltPrs) {
    if ($pr->{body}) {
        print "* [$pr->{number}]($pr->{url}): $pr->{title} ($pr->{body})\n";        
    } else {
        print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";
    }
}

print "\n\n# Closed Issues\n\n";
foreach my $pr (@$closedIssues) {
    print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";
}

sub curlCmd {
    my $url = shift;
    my $token = shift;

    my $baseCmd = "curl -H 'Accept: application/vnd.github.v3+json'";
    $baseCmd .= " -H 'Authorization: token $token'" if $token;
    $baseCmd .= " '$url' > $curlFile";

    return $baseCmd;
}

sub getPRs {
    my $baseUrl = shift;
    my $fromTime = shift;
    my $toTime = shift;

    $baseUrl .= '?state=closed&sort=created&direction=desc&per_page=100';
    
    my $page = 1;
    my $more = 0;
    
    my @mergedPrs;
    do {
        my $pullsUrl = "$baseUrl&page=$page";
        my $curlPulls = curlCmd($pullsUrl, $token);
        print STDERR "$curlPulls\n";
        system($curlPulls) and die $!;

        $more = 0;
        my $pullsJson = json_file_to_perl($curlFile);
        # TODO: error handling if this fails to parse
        foreach my $pull (@$pullsJson) {
            $more = 1;
            next unless $pull->{merged_at};
            return \@mergedPrs if $pull->{created_at} lt $fromTime;
            my %pr = (
                'url' => $pull->{html_url},
                'number' => $pull->{number},
                'title' => $pull->{title},
                'body' => $pull->{body},
                );

            # print STDERR "PR merged at $pull->{merged_at}\n";
            push (@mergedPrs, \%pr) if $pull->{merged_at} le $toTime;
        }

        $page++;
    } while $more;
    
    return \@mergedPrs;
}

sub getIssues {
    my $baseUrl = shift;
    my $fromTime = shift;
    my $toTime = shift;

    $baseUrl .= "?state=closed&sort=created&direction=desc&since=$fromTime&per_page=100";
    
    my $page = 1;
    my $more = 0;

    my @closedIssues;
    do {
        my $issuesUrl = "$baseUrl&page=$page";
        my $curlIssues = curlCmd($issuesUrl, $token);
        print STDERR "$curlIssues\n";
        system($curlIssues) and die $!;

        $more = 0;
        my $issuesJson = json_file_to_perl($curlFile);
        # TODO: error handling if this fails to parse
        foreach my $issue (@$issuesJson) {
            $more = 1;
            next unless $issue->{closed_at};
            return \@closedIssues if $issue->{created_at} lt $fromTime;
            next if $issue->{html_url} =~ m|/pull/|; # the issues API also returns PR results
            my %i = (
                'url' => $issue->{html_url},
                'number' => $issue->{number},
                'title' => $issue->{title},
                'body' => $issue->{body},
                );
            
            push (@closedIssues, \%i) if $issue->{closed_at} le $toTime; 
        }

        $page++;
    } while $more;
    
    return \@closedIssues;
}
