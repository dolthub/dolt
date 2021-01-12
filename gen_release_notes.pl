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

my ($fromTime, $fromHash, $toTime, $toHash);
foreach my $release (@$releasesJson) {
    $fromTime = $release->{created_at};
    $fromHash = $release->{target_commitish};
    last if $toTime;

    if ((! $releaseTag) || ($releaseTag eq $release->{tag_name})) {
        $toTime = $release->{created_at};
        $toHash = $release->{target_commitish};
        last unless $releaseTag;
    }
}

die "Couldn't find release" unless $toTime;

print STDERR "Looking for PRs and issues from $fromTime to $toTime\n";

my $doltPullRequestsUrl = 'https://api.github.com/repos/dolthub/dolt/pulls';
my $mergedDoltPrs = getPRs($doltPullRequestsUrl, $fromTime, $toTime);

my $doltIssuesUrl = "https://api.github.com/repos/dolthub/dolt/issues";
my $closedIssues = getIssues($doltIssuesUrl, $fromTime, $toTime);

my $fromGmsHash = getDependencyVersion("github.com/dolthub/go-mysql-server", $fromHash);
my $toGmsHash = getDependencyVersion("github.com/dolthub/go-mysql-server", $toHash);

if ($fromGmsHash ne $toGmsHash) {
    print STDERR "Looking for pulls in go-mysql-server from $fromGmsHash to $toGmsHash\n";
    my $fromGmsTime = getCommitTime("dolthub/go-mysql-server", $fromGmsHash);
    my $toGmsTime = getCommitTime("dolthub/go-mysql-server", $toGmsHash);

    my $gmsPullsUrls = 'https://api.github.com/repos/dolthub/go-mysql-server/pulls';
    my $mergedGmsPrs = getPRs($gmsPullsUrls, $fromTime, $toTime);
    my $gmsIssuesUrl = "https://api.github.com/repos/dolthub/go-mysql-server/issues";
    my $closedGmsIssues = getIssues($gmsIssuesUrl, $fromTime, $toTime);

    push @$mergedDoltPrs, @$mergedGmsPrs;
    push @$closedIssues, @$closedGmsIssues;
}

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

sub getDependencyVersion {
    my $dependency = shift;
    my $hash = shift;

    my $cmd = "git show $hash:go/go.mod | grep $dependency";
    my $line = `$cmd`;

    # TODO: this only works for commit versions, not actual releases like most software uses
    # github.com/dolthub/go-mysql-server v0.6.1-0.20210107193823-566f0ba75abc
    if ($line =~ m/\S+\s+.*-([0-9a-f]{12})/) {
        return $1;
    }

    die "Couldn't determine dependency version in $line";
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

sub getCommitTime {
    my $repo = shift;
    my $sha = shift;

    my $commitCurl = curlCmd("https://api.github.com/repos/$repo/commits?sha=$sha&per_page=1", $token);
    print STDERR "$commitCurl\n";
    system($commitCurl) and die $!;
    
    my $commitJson = json_file_to_perl($curlFile);
    # TODO: error handling if this fails to parse
    foreach my $commit (@$commitJson) {
        # [
        #  {
        #      "sha": "9a89aaf765d7868c8252d24462e371f575175658",
        #          "node_id": "MDY6Q29tbWl0MTkzNzg0MzQxOjlhODlhYWY3NjVkNzg2OGM4MjUyZDI0NDYyZTM3MWY1NzUxNzU2NTg=",
        #          "commit": {
        #              "author": {
        #                  "name": "Daylon Wilkins",
        #                      "email": "daylon@liquidata.co",
        #                      "date": "2020-12-21T14:33:08Z"
        #              },
        return $commit->{commit}{author}{date};
    }

    die "Couldn't find commit time";
}
