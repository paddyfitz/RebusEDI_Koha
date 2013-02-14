package Rebus::EDI::System::Koha;

# Copyright 2012 Mark Gavillet

use strict;
use warnings;

=head1 NAME

Rebus::EDI::System::Koha

=head1 VERSION

Version 0.01

=cut

use C4::Context;

our $VERSION='0.01';

### Evergreen
#our $edidir				=	"/tmp/";

### Koha
our $edidir				=	"$ENV{'PERL5LIB'}/misc/edi_files/";

our $ftplogfile			=	"$edidir/edi_ftp.log";
our $quoteerrorlogfile	=	"$edidir/edi_quote_error.log";
our $edi_quote_user		=	0;

sub new {
	my $class			=	shift;
	my $self			=	{};
	bless $self, $class;
	return $self;
}

sub retrieve_vendor_ftp_accounts {
	my $self	= shift;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('select vendor_edi_accounts.id as edi_account_id, 
		aqbooksellers.id as account_id, aqbooksellers.name as vendor, 
		vendor_edi_accounts.host as server, vendor_edi_accounts.username as ftpuser, 
		vendor_edi_accounts.password as ftppass, vendor_edi_accounts.in_dir as ftpdir 
		from vendor_edi_accounts inner join aqbooksellers on 
		vendor_edi_accounts.provider = aqbooksellers.id');
	$sth->execute();
	my $set = $sth->fetchall_arrayref( {} );
	my @accounts;
	my $new_account;
	foreach my $account (@$set)
	{
		$new_account	=	{
			account_id		=>	$account->{account_id},
			edi_account_id	=>	$account->{edi_account_id},
			vendor			=>	$account->{vendor},
			server			=>	$account->{server},
			ftpuser			=>	$account->{ftpuser},
			ftppass			=>	$account->{ftppass},
			ftpdir			=>	$account->{ftpdir},
			po_org_unit		=>	0,
		};
		push (@accounts,$new_account);
	}
	return @accounts;
}

sub download_quotes {
	my ($self,$ftp_accounts)=@_;
	my @local_files;
	foreach my $account (@$ftp_accounts) {	
		#get vendor details
		print "server: ".$account->{server}."\n";
		print "account: ".$account->{vendor}."\n";
		
		#get files
		use Net::FTP;
		my $newerr;
		my @ERRORS;
		my @files;
		open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
		my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
		printf EDIFTPLOG "\n\n%4d-%02d-%02d %02d:%02d:%02d\n-----\n",$year+1900,$mon+1,$mday,$hour,$min,$sec;
		print EDIFTPLOG "Connecting to ".$account->{server}."... ";
		my $ftp=Net::FTP->new($account->{server},Timeout=>10,Passive=>1) or $newerr=1;
		push @ERRORS, "Can't ftp to ".$account->{server}.": $!\n" if $newerr;
		myerr(@ERRORS) if $newerr;
		if (!$newerr)
		{
			$newerr=0;
			print EDIFTPLOG "connected.\n";

			$ftp->login($account->{ftpuser},$account->{ftppass}) or $newerr=1;
			print EDIFTPLOG "Getting file list\n";
			push @ERRORS, "Can't login to ".$account->{server}.": $!\n" if $newerr;
			$ftp->quit if $newerr;
			myerr(@ERRORS) if $newerr; 
			if (!$newerr)
			{
				print EDIFTPLOG "Logged in\n";
				$ftp->cwd($account->{ftpdir}) or $newerr=1; 
				push @ERRORS, "Can't cd in server ".$account->{server}." $!\n" if $newerr;
				myerr(@ERRORS) if $newerr;
				$ftp->quit if $newerr;

					@files=$ftp->ls or $newerr=1;
					push @ERRORS, "Can't get file list from server ".$account->{server}." $!\n" if $newerr;
					myerr(@ERRORS) if $newerr;
					if (!$newerr)
					{
						print EDIFTPLOG "Got  file list\n";   
						foreach(@files) {
							my $filename=$_;
							if ((index lc($filename),'.ceq') > -1)
							{
								my $description = sprintf "%s/%s", $account->{server}, $filename;
								print EDIFTPLOG "Found file: $description - ";
								
								# deduplicate vs. acct/filenames already in DB
		   						my $hits=find_duplicate_quotes($account->{edi_account_id},$account->{ftpdir},$filename);
		   						
								my $match=0;
								if (scalar(@$hits)) {
									print EDIFTPLOG "File already retrieved. Skipping.\n";
									$match=1;
								}
								if ($match ne 1)
								{
									chdir "$edidir";
									$ftp->get($filename) or $newerr=1;
									push @ERRORS, "Can't transfer file ($filename) from ".$account->{server}." $!\n" if $newerr;
									$ftp->quit if $newerr;
									myerr(@ERRORS) if $newerr;
									if (!$newerr)
									{
										print EDIFTPLOG "File retrieved\n";
										open FILE,"$edidir/$filename" or die "Couldn't open file: $!\n";
										my $message_content=join("",<FILE>);
										close FILE;
										my $logged_quote=LogQuote($message_content, $account->{ftpdir}."/".$filename, $account->{server}, $account->{edi_account_id});
										my $quote_file	=	{
											filename	=>	$filename,
											account_id	=>	$account->{account_id},
											po_org_unit	=>	$account->{po_org_unit},
											edi_quote_user	=>	$edi_quote_user,
											logged_quote_id	=>	$logged_quote,
											edi_account_id	=>	$account->{edi_account_id},
										};
										push (@local_files,$quote_file);										
									}
								}
							}
						}
					}
			}

			$ftp->quit;
		}
		$newerr=0;
	}
	return @local_files;
}

sub myerr {
	my @ERRORS= shift;
	open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
	print EDIFTPLOG "Error: ";
	print EDIFTPLOG @ERRORS;
	close EDIFTPLOG;
}

sub find_duplicate_quotes {
	my ($edi_account_id, $ftpdir, $filename)	= @_;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('select edifact_messages.key from edifact_messages 
		inner join vendor_edi_accounts on vendor_edi_accounts.provider=edifact_messages.provider 
		where vendor_edi_accounts.id=? and edifact_messages.remote_file=? and status<>?');
	$sth->execute($edi_account_id,$ftpdir."/".$filename,'Processed');
	my $hits = $sth->fetchall_arrayref( {} );
	return $hits;
}

# updates last activity in acq.edi_account and writes a new entry to acq.edi_message
sub LogQuote {
	my ($content, $remote, $server, $account_or_id) = @_;
	$content or return;
	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
	my $last_activity=sprintf("%4d-%02d-%02d",$year+1900,$mon+1,$mday);
	my $account = record_activity( $account_or_id,$last_activity );
	my $message_type=($content =~ /'UNH\+\w+\+(\S{6}):/) ? $1 : 'QUOTES';
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('insert into edifact_messages (message_type, date_sent, provider, 
		status, edi, remote_file) values (?,?,?,?,?,?)');
	$sth->execute($message_type,$last_activity,$account,'Received',$content,$remote);
	my $insert_id = $dbh->last_insert_id(undef,undef,qw(edifact_messages key),undef);
	
	return $insert_id;
}

sub update_quote_status {
	my ($quote_id, $edi_account_id,$basketno) = @_;
	my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
	my $last_activity=sprintf("%4d-%02d-%02d",$year+1900,$mon+1,$mday);
	my $account = record_activity( $edi_account_id,$last_activity );
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('update edifact_messages set edifact_messages.status=?, 
		basketno=? where edifact_messages.key=?');
	$sth->execute('Processed',$basketno,$quote_id);
}

sub record_activity {
	my ($account_or_id,$last_activity) = @_;
	$account_or_id or return;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('update vendor_edi_accounts set last_activity=? where 
		id=?');
	$sth->execute($last_activity,$account_or_id);
	$sth = $dbh->prepare('select provider from vendor_edi_accounts where id=?');
	$sth->execute($account_or_id);
	my @result;
	my $provider;
	while (@result = $sth->fetchrow_array())
	{
		$provider=$result[0];
	}
	return $provider;
}

sub process_quotes {
	my ($self, $quotes)	= @_;
	foreach my $quote (@$quotes)
	{
		my $vendor_san = get_vendor_san($quote->{account_id});
		my $module=get_vendor_module($vendor_san);
		$module or return;
		require "Rebus/EDI/Vendor/$module.pm";
		$module="Rebus::EDI::Vendor::$module";
		import $module;
		my $vendor_module=$module->new();
		my @parsed_quote=$vendor_module->parse_quote($quote);
		use C4::Acquisition;
		use C4::Biblio;
		use C4::Items;
		my $order_id=NewBasket($quote->{account_id}, 0, $quote->{filename}, '', '', '');
		
		foreach my $item (@parsed_quote)
		{
			foreach my $copy (@{$item->{copies}})
			{
				my $quote_copy	=	{
					author			=>	$item->{author},
					price			=>	$item->{price},
					ecost			=>	get_discounted_price($quote->{account_id},$item->{price}),
					llo				=>	$copy->{llo},
					lfn				=>	$copy->{lfn},
					lsq				=>	$copy->{lsq},
					lst				=>	$copy->{lst},
					lcl				=>	$copy->{lcl},
					budget_id		=>	get_budget_id($copy->{lfn}),
					title			=>	$item->{title},
					isbn			=>	$item->{isbn},
					publisher		=>	$item->{publisher},
					year			=>	$item->{year},
				};
				use Rebus::EDI::Custom::Default;
				my $local_transform=Rebus::EDI::Custom::Default->new();
				my $koha_copy=$local_transform->transform_local_quote_copy($quote_copy);
				
				my $lsq_identifier=$local_transform->lsq_identifier();
				# create biblio record
				my $record = TransformKohaToMarc(
		        {
		            "biblio.title"                => $koha_copy->{title},
		            "biblio.author"               => $koha_copy->{author}		? $koha_copy->{author}		: "",
		            "biblio.seriestitle"          => "",
		            "biblioitems.isbn"            => $koha_copy->{isbn}			? $koha_copy->{isbn}		: "",
		            "biblioitems.publishercode"   => $koha_copy->{publisher}	? $koha_copy->{publisher}	: "",
		            "biblioitems.publicationyear" => $koha_copy->{year}			? $koha_copy->{year}		: "",
		            "biblio.copyrightdate"        => $koha_copy->{year}			? $koha_copy->{year}		: "",
		            "biblioitems.cn_source"		  => "ddc",
		            "items.cn_source"			  => "ddc",
		            "items.notforloan"			  => "-1",
					"items.$lsq_identifier"			=>	$koha_copy->{lsq},
		            "items.homebranch"			  => $koha_copy->{llo},
		            "items.holdingbranch"		  => $koha_copy->{llo},
		            "items.booksellerid"		  => $quote->{account_id},
		            "items.price"				  => $koha_copy->{price},
		            "items.replacementprice"	  => $koha_copy->{price},
		            "items.itemcallnumber"		  => $koha_copy->{lcl},
		            "items.itype"				  => $koha_copy->{lst},
		            "items.cn_sort"				  => "",
		        });
				
				#check if item already exists in catalogue
				my ($biblionumber,$bibitemnumber)=check_order_item_exists($item->{isbn});
				
				if (!defined $biblionumber)
				{
		        	# create the record in catalogue, with framework ''
		        	($biblionumber,$bibitemnumber) = AddBiblio($record,'');
	        	}
				
				# create order line
				my %orderinfo = (
	        		basketno				=> $order_id,
		        	ordernumber				=> "",
		        	subscription			=> "no",
		        	uncertainprice			=> 0,
		        	biblionumber			=> $biblionumber,
		        	title					=> $koha_copy->{title},
		        	quantity				=> 1,
		        	biblioitemnumber		=> $bibitemnumber,
		        	rrp						=> $koha_copy->{price},
		        	ecost					=> $koha_copy->{ecost},
		 	       	sort1					=> "",
		 	       	sort2					=> "",
		        	booksellerinvoicenumber	=> $item->{item_reference},
		        	listprice				=> $koha_copy->{price},
		        	branchcode				=> $koha_copy->{llo},
		        	budget_id				=> $koha_copy->{budget_id},
		        );
        
		        my $orderinfo = \%orderinfo;
        
		        my ($retbasketno, $ordernumber ) = NewOrder($orderinfo); 
		        
		        # now, add items if applicable
			    if (C4::Context->preference('AcqCreateItem') eq 'ordering')
			    {
			    	my $itemnumber;
			    	($biblionumber,$bibitemnumber,$itemnumber) = AddItemFromMarc($record,$biblionumber);
		            NewOrderItem($itemnumber, $ordernumber);
			    }
			}
		}
		update_quote_status($quote->{logged_quote_id},$quote->{edi_account_id},$order_id);
		### manipulate quote file on remote server
		my $vendor_ftp_account=get_vendor_ftp_account($quote->{edi_account_id});
		$vendor_module->post_process_quote_file($quote->{filename},$vendor_ftp_account);
		return 1;

	}
}

sub get_vendor_ftp_account {
	my $edi_account_id	= shift;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('select host,username,password,in_dir from vendor_edi_accounts 
		where id=?');
	$sth->execute($edi_account_id);
	my @result;
	my $account;
	while (@result=$sth->fetchrow_array())
	{
		$account = {
			host		=>	$result[0],
			username	=>	$result[1],
			password	=>	$result[2],
			in_dir		=>	$result[3],
		};
	}
	return $account;
}

sub get_discounted_price {
	my ($booksellerid, $price) = @_;
	my $dbh = C4::Context->dbh;
	my @discount;
	my $ecost;
	my $percentage;
	my $sth = $dbh->prepare('select discount from aqbooksellers where id=?');
	$sth->execute($booksellerid);
	while (@discount=$sth->fetchrow_array())
	{
		$percentage=$discount[0];
	}
	$ecost=($price-(($percentage*$price)/100));
	return $ecost;
}

sub get_budget_id {
	my $fundcode=shift;
	my $dbh = C4::Context->dbh;
	my @funds;
	my $ecost;
	my $budget_id;
	my $sth = $dbh->prepare('select budget_id from aqbudgets where budget_code=?');
	$sth->execute($fundcode);
	while (@funds=$sth->fetchrow_array())
	{
		$budget_id=$funds[0];
	}
	return $budget_id;
}

sub get_vendor_san {
	my $vendor_id=shift;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare('select san from vendor_edi_accounts where provider=?');
	$sth->execute($vendor_id);
	my @result;
	my $san;
	while (@result = $sth->fetchrow_array())
	{
		$san=$result[0];
	}
	return $san;
}

sub get_vendor_module {
	my $san=shift;
	my $module;
	use Rebus::EDI;
	my @vendor_list=Rebus::EDI::list_vendors();
	foreach my $vendor (@vendor_list)
	{
		if ($san eq $vendor->{san} || $san eq $vendor->{ean})
		{
			$module=$vendor->{module};
			last;
		}
	}
	return $module;
}

sub check_order_item_exists {
	my $isbn=shift;
	my $dbh = C4::Context->dbh;
	my $sth;
	my @matches;
	my $biblionumber;
	my $bibitemnumber;
	$sth = $dbh->prepare('select biblionumber, biblioitemnumber from biblioitems where isbn=?');
	$sth->execute($isbn);
	while (@matches=$sth->fetchrow_array())
	{
		$biblionumber=$matches[0];
		$bibitemnumber=$matches[1];
	}
	if ($biblionumber)
	{
		return $biblionumber,$bibitemnumber;
	}
	else
	{
		use Rebus::EDI;
		use Business::ISBN;
		my $edi=Rebus::EDI->new();
		$isbn=$edi->cleanisbn($isbn);
		if (length($isbn)==10)
		{
			$isbn=Business::ISBN->new($isbn);
			if ($isbn)
			{
				if ($isbn->is_valid)
				{
					$isbn=($isbn->as_isbn13)->isbn;
					$sth->execute($isbn);
					while (@matches=$sth->fetchrow_array())
					{
						$biblionumber=$matches[0];
						$bibitemnumber=$matches[1];
   					}
				}
			}
		}
		elsif (length($isbn)==13)
		{
			$isbn=Business::ISBN->new($isbn);
			if ($isbn)
			{
				if ($isbn->is_valid)
				{
					$isbn=($isbn->as_isbn10)->isbn;
					$sth->execute($isbn);
					while (@matches=$sth->fetchrow_array())
					{
						$biblionumber=$matches[0];
						$bibitemnumber=$matches[1];
   					}
				}
			}
		}
		return $biblionumber,$bibitemnumber;
	}
}

sub retrieve_orders {
	my ($self,$order_id)	= @_;
	my $dbh = C4::Context->dbh;
	my @active_orders;
	           
	## retrieve basic order details
	my $sth=$dbh->prepare('select booksellerid as provider from aqbasket where basketno=?');
	$sth->execute($order_id);
	my $orders = $sth->fetchall_arrayref( {} );
	
	foreach my $order (@{$orders})
	{
		push @active_orders,{order_id=>$order_id,provider_id=>$order->{provider}};
	}
	return \@active_orders;
}

sub retrieve_order_details {
	my ($self,$orders,$ean)	= @_;
	my @fleshed_orders;
	foreach my $order (@{$orders})
	{
		my $fleshed_order;
		$fleshed_order={order_id=>$order->{order_id},provider_id=>$order->{provider_id}};
		
		## retrieve module for vendor
		my $dbh = C4::Context->dbh;
	    my $sth=$dbh->prepare('select san from vendor_edi_accounts where provider=?');
		$sth->execute($order->{provider_id});
		my @result;
		my $san;
		while (@result = $sth->fetchrow_array())
		{
			$san=$result[0];
		}
		$fleshed_order->{'module'}=get_vendor_module($san);
		$fleshed_order->{'san_or_ean'}=$san;
		$fleshed_order->{'org_san'}=$ean;
		$fleshed_order->{'quote_or_order'}=quote_or_order($order->{order_id});
		my @lineitems=get_order_lineitems($order->{order_id});
		$fleshed_order->{'lineitems'}=\@lineitems;
		
		push @fleshed_orders,$fleshed_order;
	}
	return \@fleshed_orders;
}

sub create_order_file {
	my ($self,$order_message,$order_id)=@_;
	my $filename="$edidir/ediorder_$order_id.CEP";
	open(EDIORDER,">$filename");
	print EDIORDER $order_message;
	close EDIORDER;
	my $vendor_ftp_account=get_vendor_ftp_account_by_order_id($order_id);
	my $sent_order=send_order_message($filename,$vendor_ftp_account,$order_message,$order_id);
	return $filename;
}

sub get_vendor_ftp_account_by_order_id {
	my $order_id=shift;
	my $vendor_ftp_account;
	my @result;
	my $dbh = C4::Context->dbh;
	my $sth=$dbh->prepare('select vendor_edi_accounts.* from vendor_edi_accounts, aqbasket 
		where vendor_edi_accounts.provider=aqbasket.booksellerid and aqbasket.basketno=?');
	$sth->execute($order_id);
	while (@result = $sth->fetchrow_array())
	{
		$vendor_ftp_account->{id}				=	$result[0];
		$vendor_ftp_account->{label}			=	$result[1];
		$vendor_ftp_account->{host}				=	$result[2];
		$vendor_ftp_account->{username}			=	$result[3];
		$vendor_ftp_account->{password}			=	$result[4];
		$vendor_ftp_account->{path}				=	$result[7];
		$vendor_ftp_account->{last_activity}	=	$result[5];
		$vendor_ftp_account->{provider}			=	$result[6];
		$vendor_ftp_account->{in_dir}			=	$result[7];
		$vendor_ftp_account->{edi_account_id}	=	$result[0];
	}
	return $vendor_ftp_account;
}

sub send_order_message {
	my ($filename,$ftpaccount,$order_message,$order_id)=@_;
	my @ERRORS;
	my $newerr;
	my $result;
	
	open(EDIFTPLOG,">>$ftplogfile") or die "Could not open $ftplogfile\n";
		my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
		printf EDIFTPLOG "\n\n%4d-%02d-%02d %02d:%02d:%02d\n-----\n",$year+1900,$mon+1,$mday,$hour,$min,$sec;
		
	# check edi order file exists
	if (-e $filename)
	{
		use Net::FTP;
		
		print EDIFTPLOG "Connecting to ".$ftpaccount->{host}."... ";
		# connect to ftp account
		my $ftp=Net::FTP->new($ftpaccount->{host},Timeout=>10,Passive=>1) or $newerr=1;
		push @ERRORS, "Can't ftp to ".$ftpaccount->{host}.": $!\n" if $newerr;
		myerr(@ERRORS) if $newerr;
		if (!$newerr)
		{
			$newerr=0;
			print EDIFTPLOG "connected.\n";
			
			# login
			$ftp->login("$ftpaccount->{username}","$ftpaccount->{password}") or $newerr=1;
			$ftp->quit if $newerr;
			print EDIFTPLOG "Logging in...\n";
			push @ERRORS, "Can't login to ".$ftpaccount->{host}.": $!\n" if $newerr;
			myerr(@ERRORS) if $newerr;
			if (!$newerr)
			{
				print EDIFTPLOG "Logged in\n";
				# cd to directory
				$ftp->cwd("$ftpaccount->{path}") or $newerr=1; 
				push @ERRORS, "Can't cd in server ".$ftpaccount->{host}." $!\n" if $newerr;
				myerr(@ERRORS) if $newerr;
				$ftp->quit if $newerr;
				
				# put file
				if (!$newerr)
				{
					$newerr=0;
   					$ftp->put($filename) or $newerr=1;
   					push @ERRORS, "Can't write order file to server ".$ftpaccount->{host}." $!\n" if $newerr;
					myerr(@ERRORS) if $newerr;
					$ftp->quit if $newerr;
   					if (!$newerr)
   					{
   						print EDIFTPLOG "File: $filename transferred successfully\n";
   						$ftp->quit;
   						unlink($filename);
   						record_activity($ftpaccount->{id});
   						my $pos=rindex($filename,"/");
   						log_order($order_message,$ftpaccount->{path}.substr($filename,$pos),$ftpaccount->{provider},$order_id);
   						
   						return $result;
   					}
   				}			
			}			
		}
	}
	else
	{
		print EDIFTPLOG "Order file $filename does not exist\n";
	}
}

sub log_order {
    my ($content, $remote, $edi_account_id, $order_id) = @_;
    my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time);
    my $date_sent=sprintf("%4d-%02d-%02d",$year+1900,$mon+1,$mday);

	my $dbh = C4::Context->dbh;
	my $sth=$dbh->prepare('insert into edifact_messages (message_type,date_sent,provider,status,basketno,
		edi,remote_file) values (?,?,?,?,?,?,?)');
	$sth->execute('ORDER',$date_sent,$edi_account_id,'Sent',$order_id,$content,$remote);
}

sub quote_or_order {
	my $order_id=shift;
	my @result;
	my $quote_or_order;
	my $dbh = C4::Context->dbh;
	my $sth=$dbh->prepare('select edifact_messages.key from edifact_messages 
		where basketno=? and message_type=?');
	$sth->execute($order_id,'QUOTES');
	if ($sth->rows==0)
	{
		$quote_or_order='o';
	}
	else
	{
		$quote_or_order='q';
	}
	return $quote_or_order;
}

sub get_order_lineitems {
	my $order_id=shift;
	use C4::Acquisition;
	my @lineitems = GetOrders($order_id);
	my @fleshed_lineitems;
	foreach my $lineitem (@lineitems)
	{
		use Rebus::EDI;
		my $clean_isbn=Rebus::EDI::cleanisbn($lineitem->{isbn});
		my $fleshed_lineitem;
		$fleshed_lineitem->{binding}	=	'O';
		$fleshed_lineitem->{currency}	=	'GBP';
		$fleshed_lineitem->{id}			=	$lineitem->{ordernumber};
		$fleshed_lineitem->{qli}		=	$lineitem->{booksellerinvoicenumber};
		$fleshed_lineitem->{rff}		=	$order_id."/".$fleshed_lineitem->{id};
		$fleshed_lineitem->{isbn}		=	$clean_isbn;
		$fleshed_lineitem->{title}		=	$lineitem->{title};
		$fleshed_lineitem->{author}		=	$lineitem->{author};
		$fleshed_lineitem->{publisher}	=	$lineitem->{publishercode};
		$fleshed_lineitem->{year}		=	$lineitem->{copyrightdate};
		$fleshed_lineitem->{price}		=	sprintf "%.2f",$lineitem->{listprice};
		$fleshed_lineitem->{quantity}	=	get_order_quantity($lineitem->{ordernumber});
		
		my @lineitem_copies;
		my $fleshed_lineitem_detail;
		my ($branchcode,$callnumber,$itype,$location,$fund) = get_lineitem_additional_info($lineitem->{ordernumber});
		$fleshed_lineitem_detail->{llo}		=	$branchcode;
		$fleshed_lineitem_detail->{lfn}		=	$fund;
		$fleshed_lineitem_detail->{lsq}		=	$location;
		$fleshed_lineitem_detail->{lst}		=	$itype;
		$fleshed_lineitem_detail->{lcl}		=	$callnumber;
		$fleshed_lineitem_detail->{note}	=	$lineitem->{notes};
		push (@lineitem_copies,$fleshed_lineitem_detail);
		
		$fleshed_lineitem->{copies}		=	\@lineitem_copies;
		push (@fleshed_lineitems,$fleshed_lineitem);
	}
	return @fleshed_lineitems;
}

sub get_order_quantity {
	my $ordernumber=shift;
	my @rows;
	my $quantity;
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare(" SELECT count(*) FROM aqorders_items inner join 
		items on aqorders_items.itemnumber=items.itemnumber WHERE 
		aqorders_items.ordernumber=?");
	$sth->execute($ordernumber);
	while (@rows=$sth->fetchrow_array())
	{
		$quantity=$rows[0];
	}
	return $quantity;
}

sub get_lineitem_additional_info {
	my $ordernumber=shift;
	my @rows;
	my $homebranch;
	my $callnumber;
	my $itype;
	my $location;
	my $fund;
	use Rebus::EDI::Custom::Default;
	my $local_transform=Rebus::EDI::Custom::Default->new();
	my $lsq_identifier=$local_transform->lsq_identifier();
	my $dbh = C4::Context->dbh;
	my $sth = $dbh->prepare("select items.homebranch, items.itemcallnumber, items.itype, 
		items.$lsq_identifier from items inner join aqorders_items on 
		aqorders_items.itemnumber=items.itemnumber where aqorders_items.ordernumber=?");
	$sth->execute($ordernumber);
	while (@rows=$sth->fetchrow_array())
	{
		$homebranch=$rows[0];
		$callnumber=$rows[1];
		$itype=$rows[2];
		$location=$rows[3];
	}
	$sth = $dbh->prepare("select aqbudgets.budget_code from aqbudgets inner join aqorders on 
		aqorders.budget_id=aqbudgets.budget_id where aqorders.ordernumber=?");
	$sth->execute($ordernumber);
	while (@rows=$sth->fetchrow_array())
	{
		$fund=$rows[0];
	}
	return $homebranch,$callnumber,$itype,$location,$fund;
}

1;