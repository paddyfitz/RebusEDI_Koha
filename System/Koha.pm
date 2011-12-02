package RebusEDI::System::Koha;

# Copyright 2011 Mark Gavillet

use strict;
use warnings;

=head1 NAME

RebusEDI::System::Koha

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




1;