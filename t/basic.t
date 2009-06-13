#!/usr/bin/perl

use strict;
use warnings;

use Test::More 'no_plan';
use Test::Moose;

use ok "Net::Amazon::S3::CAS::BLOB";
use ok "Net::Amazon::S3::CAS::BLOB::String";
use ok "Net::Amazon::S3::CAS::BLOB::File";
use ok "Net::Amazon::S3::CAS::Entry";
use ok "Net::Amazon::S3::CAS::Collection::Simple";
use ok "Net::Amazon::S3::CAS";

{
    package MockBucket;
    use Moose;

    use MooseX::Types::Moose qw(Str HashRef);
    use MooseX::Types::Structured qw(Dict);
    use MooseX::AttributeHelpers;

    use Path::Class qw(file);

    use Test::More;

    use namespace::clean -except => "meta";;

    use asa 'Net::Amazon::S3::Bucket';

    sub bucket { "foo" }

    has storage => (
        metaclass => "Collection::Hash",
        isa => HashRef[HashRef],
        is  => "ro",
        provides => {
            set    => "_add_key",
            get    => "get_key",
            delete => "delete_key",
        },
        default => sub { +{} },
    );

    sub add_key {
        my ( $self, $key, $data, $headers ) = @_;

        my %headers;

        if ( $headers ) {
            @headers{map { lc } keys %$headers} = values %$headers;
        }

        $self->_add_key( $key => {
            %headers,
            value => $data,
            "content-length" => length($data),
        } );
    }

    sub add_key_filename {
        my ( $self, $key, $filename, $headers ) = @_;

        $self->add_key( $key, scalar(file($filename)->slurp), $headers );
    }

    sub head_key {
        my ( $self, $key ) = @_;
        $self->get_key($key);
    }

    sub assert_empty {
        my $self = shift;

        local $Test::Builder::Level = $Test::Builder::Level + 1;

        is_deeply( $self->storage, {}, "bucket is empty" ); 
    }

    sub has_key {
        my ( $self, $key ) = @_;

        local $Test::Builder::Level = $Test::Builder::Level + 1;

        ok( $self->storage->{$key}, "$key exists" );
    }

    sub list_all {
        my $self = shift;
        return { keys => [ map { { key => $_ } } sort keys %{ $self->storage } ] };
    }
}

{
    my $blob = Net::Amazon::S3::CAS::BLOB::String->new( data => "henry" );

    is( $blob->slurp, "henry", "blob data" );

    my $fh = $blob->openr;

    my $data = do { local $/; <$fh> };

    is( $data, "henry", "handle" );

    is( $blob->size, 5, "size" );
}

{
    my $blob = Net::Amazon::S3::CAS::BLOB::File->new( file => __FILE__ );

    my $data = $blob->slurp;

    like( $data, qr/this is a magic string, you wouldn't find it elsewhere/, "slurped file" );

    ok( -e $blob->filename, "exists" );
    is( $blob->filename, __FILE__, "filename" );
    ok( !ref($blob->filename), "stringified" );

    is( $blob->size, -s __FILE__, "size using stat" );
    is( $blob->size, length($data), "equal to length of slurped" );

    my $fh = $blob->openr;

    my $read = do { local $/; <$fh> };

    is( $data, $read, "working handle" );
}

{
    my $str = Net::Amazon::S3::CAS::Entry->new(
        key => "foo",
        blob => Net::Amazon::S3::CAS::BLOB::String->new( data => "foo" ),
    );

    is( $str->slurp, "foo", "delegates BLOB api" );

    my $file = Net::Amazon::S3::CAS::Entry->new(
        key => "basic.t",
        blob => Net::Amazon::S3::CAS::BLOB::File->new( file => __FILE__ ),
    );

    my $c = Net::Amazon::S3::CAS::Collection::Simple->new( entries => [ $str, $file ] );

    does_ok( $c->entries, "Data::Stream::Bulk", "entry stream" );

    is_deeply( [ sort $c->entries->all ], [ sort $str, $file ], "all entries" );

    my $bucket = MockBucket->new;

    my $cas = Net::Amazon::S3::CAS->new(
        prefix => "blobs/",
        bucket => $bucket,
        collection => $c,
        prune => 1,
    );

    $bucket->assert_empty;

    $cas->sync;

    $bucket->has_key("blobs/foo");
}

