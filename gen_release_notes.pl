#!/usr/bin/perl

use warnings;
use strict;

use open ":std", ":encoding(UTF-8)";

use JSON::Parse 'json_file_to_perl';
use Data::Dumper;

use Getopt::Long;

# Usage: ./gen_release_notes.pl <version> to generate release notes
# for an existing release
#
# ./gen_release_notes.pl to generate release notes for a new release
# (everything since the last release)
# 
# Example: ./gen_release_notes.pl v0.22.9

# GitHub API rate limits unauthed access to 60 requests an
# hour. Running this to generate release notes for a single release
# shouldn't normally hit this limit. If it does, you'll need to use a
# personal access token with the --token flag. Details for how to
# create a token:
# https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/creating-a-personal-access-token
my $token = "";
GetOptions ("token|t=s" => \$token) or die "Error in command line args";

# no arg for changes since last release
my $releaseTag = shift @ARGV;

print STDERR "Looking for changes for release $releaseTag\n" if $releaseTag;

my $tmpDir = "/var/tmp";
my $curlFile = "$tmpDir/curl-$$.out";

my $doltReleasesUrl = 'https://api.github.com/repos/dolthub/dolt/releases';
my $curlReleases = curlCmd($doltReleasesUrl, $token);

print STDERR "$curlReleases\n";
system($curlReleases) and die $!;

my $releasesJson = json_file_to_perl($curlFile);

my ($fromTime, $fromTag, $fromHash, $toTime, $toTag, $toHash);
foreach my $release (@$releasesJson) {
    $fromTime = $release->{created_at};
    $fromTag = $release->{tag_name};
    last if $toTime;

    if ((! $releaseTag) || ($releaseTag eq $release->{tag_name})) {
        $toTime = $release->{created_at};
        $toTag = $release->{tag_name};
        last unless $releaseTag;
    }
}

die "Couldn't find release" unless $toTime;

$fromHash = tagToCommitHash($fromTag);
$toHash = tagToCommitHash($toTag);

# If we don't have a release tag to generate notes for, there is no
# upper bound for pulls and issues, only a lower bound.
$toTime = "" unless $releaseTag;

print STDERR "Looking for PRs and issues from $fromTime to $toTime\n";

my $doltPullRequestsUrl = 'https://api.github.com/repos/dolthub/dolt/pulls';
my $mergedPrs = getPRs($doltPullRequestsUrl, $fromTime, $toTime);

my $doltIssuesUrl = "https://api.github.com/repos/dolthub/dolt/issues";
my $closedIssues = getIssues($doltIssuesUrl, $fromTime, $toTime);

my $fromGmsHash = getDependencyVersion("github.com/dolthub/go-mysql-server", $fromHash);
my $toGmsHash = getDependencyVersion("github.com/dolthub/go-mysql-server", $toHash);

if ($fromGmsHash ne $toGmsHash) {
    print STDERR "Looking for pulls in go-mysql-server from $fromGmsHash to $toGmsHash\n";
    my $fromGmsTime = getCommitTime("dolthub/go-mysql-server", $fromGmsHash);
    my $toGmsTime = getCommitTime("dolthub/go-mysql-server", $toGmsHash);

    my $gmsPullsUrls = 'https://api.github.com/repos/dolthub/go-mysql-server/pulls';
    my $mergedGmsPrs = getPRs($gmsPullsUrls, $fromGmsTime, $toGmsTime);
    my $gmsIssuesUrl = "https://api.github.com/repos/dolthub/go-mysql-server/issues";
    my $closedGmsIssues = getIssues($gmsIssuesUrl, $fromGmsTime, $toGmsTime);

    push @$mergedPrs, @$mergedGmsPrs;
    push @$closedIssues, @$closedGmsIssues;
}

print "# Merged PRs\n\n";
foreach my $pr (@$mergedPrs) {
    print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";

    if ($pr->{body}) {
        my @lines = split (/\s*[\n\r]+\s*/, $pr->{body});
        foreach my $line (@lines) {
            print "  $line\n";
        }
    }
}

print "\n# Closed Issues\n\n";
foreach my $pr (@$closedIssues) {
    print "* [$pr->{number}]($pr->{url}): $pr->{title}\n";
}

exit 0;

# Gets a curl command to access the github api with the URL with token given (optional).
sub curlCmd {
    my $url = shift;
    my $token = shift;

    my $baseCmd = "curl -H 'Accept: application/vnd.github.v3+json'";
    $baseCmd .= " -H 'Authorization: token $token'" if $token;
    $baseCmd .= " '$url' > $curlFile";

    return $baseCmd;
}

# Returns a list of closed PRs in the time range given, using the 
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
        die "JSON file does not contain a list response" unless ref($pullsJson) eq 'ARRAY';
        
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
            push (@mergedPrs, \%pr) if !$toTime || $pull->{merged_at} le $toTime;
        }

        $page++;
    } while $more;
    
    return \@mergedPrs;
}

# Returns the SHA version of the dependency named at the repository SHA given.
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

# Returns a list of closed issues in the time frame given from the github API url given
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

        my $issuesJson = json_file_to_perl($curlFile);
        die "JSON file does not contain a list response" unless ref($issuesJson) eq 'ARRAY';

        $more = 0;
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
            
            push (@closedIssues, \%i) if !$toTime || $issue->{closed_at} le $toTime; 
        }

        $page++;
    } while $more;
    
    return \@closedIssues;
}

# Returns the commit time for the commit from the repo given with the sha given
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

sub tagToCommitHash {
    my $tag = shift;

    my $line = `git rev-list -n 1 $tag`;
    
    if ($line =~ m/([0-9a-f]+)/) {
        return $1;
    }

    die "Couldn't determine dependency commit hash for tag $tag";
}
