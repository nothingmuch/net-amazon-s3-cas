#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';
use Test::Moose;

use ok 'Net::Amazon::S3::CAS::Directory::Simple';

{
    my $dir = Net::Amazon::S3::CAS::Directory::Simple->new(
        dir => "lib",
        file_filter => sub { -s $_ and $_->basename !~ /\~$/ and $_->basename !~ /^\./ },
    );

    does_ok( $dir, "Net::Amazon::S3::CAS::Collection" );

    my $entries = $dir->entries;

    does_ok( $entries, "Data::Stream::Bulk" );

    my @entries = $entries->all;

    my %keys;
    my %names;

    foreach my $entry ( @entries ) {
        isa_ok( $entry, "Net::Amazon::S3::CAS::Entry" );

        $names{$entry->name}++;
        $keys{$entry->key}++;
    }

    is( scalar(keys %names), scalar(@entries), "all names are unique" );

    is( scalar(keys %keys), scalar(@entries), "all keys are unique" );

    foreach my $key ( keys %keys ) {
        like( $key, qr/^[a-f0-9]{40}$/i, "key is a SHA1" );
    }
}
