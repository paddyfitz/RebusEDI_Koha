package Rebus::EDI::Custom::Default;

# Copyright 2012 Mark Gavillet

use strict;
use warnings;

use parent qw(Exporter);

=head1 NAME

Rebus::EDI::Custom::Default

=head1 VERSION

Version 0.01

=cut

our $VERSION='0.01';

sub new {
	my $class			=	shift;
	my $self			=	{};
	bless $self, $class;
	return $self;
}

sub transform_local_quote_copy {
	my ($self, $item) =	@_;
	
	### default - return the item without transformations
	return $item;
}

sub lsq_identifier {
	### use CCODE authorised values in LSQ segment
	# return 'ccode';
	
	### use LOC authorised values in LSQ segment
	return 'location';
}

1;