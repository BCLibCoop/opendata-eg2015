#!/usr/bin/perl

use strict;
use warnings;
use OpenSRF::AppSession;
use OpenSRF::System;
use XML::LibXML;
use XML::LibXML::XPathContext;
use JSON::XS;
use Encode;

use Data::Dumper;

OpenSRF::System->bootstrap_client(config_file => '/srv/openils/conf/opensrf_core.xml');

my $session = OpenSRF::AppSession->create("open-ils.cstore");

my $parser = XML::LibXML->new();

my $xpc = XML::LibXML::XPathContext->new();

$xpc->registerNs('marcxml', 'http://www.loc.gov/MARC21/slim');

my $trim_re = qr/^\s+|\s+$/;

my $fh;

open($fh, ">:encoding(UTF-8)", "coop_opendata_popular_books.tsv") or die "Cannot open coop_opendata_popular_books.tsv";

my $geo_json_fh;

open($geo_json_fh, ">:encoding(UTF-8)", "coop_geo_json_opendata_popular_books.json") or die "Cannot open coop_geo_json_opendata_popular_books.json";

my $average_price_json_fh;

open($average_price_json_fh, ">:encoding(UTF-8)", "coop_geo_json_average_price_per_library.json") or die "Cannot open coop_geo_json_average_price_per_library.json";

output_geo_json_header($geo_json_fh);

output_geo_json_header($average_price_json_fh);

#eventually we will be looping over the contents of the data from the DB

#to do the looping place all DB files into a directory.  Get a dir listing and loop over each file.

#pull out record id, lending count, ou id, library name and geo location from file data 
#(we must add this data to the files before we can process
#this will be done via sed or something similar once the approriate geo data is located)
#a second loop will be needed to loop over each DB file filehandle. In this loop we will extract the record id
#and pull out the data from the MARCXML.

my $data_creation_date = localtime();

my $library_lending_data_fh;

my $dh;

opendir ($dh, 'data') or die "Cannot open $dh";

my $first_feature = 1;

my $first_average_price_feature = 1;

while(readdir $dh) {

  if ($_ =~ /.txt$/) {

    open($library_lending_data_fh, "<", "./data/$_") or die "Cannot open file with lending data";

    my @all_prices = ();

    my @all_lends = ();

    my $work_count = 0;

    my ($record, $library_ou_id, $lending_count, $library_name, $library_geo_location) = ();

    while(<$library_lending_data_fh>) {

      $work_count++;

      my @data = split('\|', $_);

      map {$_ =~ s/$trim_re//g} @data;

      ($record, $library_ou_id, $lending_count, $library_name, $library_geo_location) = @data;

      $library_geo_location = fix_geo_location_data($library_geo_location);

      if ($record) {
        my $result = $session->request("open-ils.cstore.direct.biblio.record_entry.retrieve", $record);

        my $request = $result->gather();

        my ($title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date);

        if ($request) {
          my $record_xml = $parser->parse_string($request->[11]);

          ($title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date) = extract_data($record_xml);

        } else {
          ($title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date) = ([],[],[],[],[],[]);
        }

        push(@all_prices, extract_price($prices));

        push(@all_lends, $lending_count);

        output_records($fh, $geo_json_fh, $library_ou_id, $library_name, $library_geo_location, $lending_count, $data_creation_date, $record, $title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date, $first_feature);

        $first_feature = 0;
      }
    }

    #After averaging the prices call output_records with a new file handle here
    #to create another GeoJSON file with price averages.
    
    my $average_price_per_library = average_prices(@all_prices);

    my $average_lending_per_work = average_lends(@all_lends, $work_count);

    output_records($fh, $average_price_json_fh, $library_ou_id, $library_name, $library_geo_location, '', $data_creation_date, '', [], [], [], [], [], [], $first_average_price_feature, $average_price_per_library, $average_lending_per_work);

    $first_average_price_feature = 0;

    close $library_lending_data_fh;
  }
}

#the loop will end here
output_geo_json_footer($geo_json_fh);
output_geo_json_footer($average_price_json_fh);

close $fh;

close $geo_json_fh;

close $average_price_json_fh;

$session->disconnect();

sub extract_data {

  my ($record_xml) = @_;

  my (@title, @authors, @prices, @subject_headings, @isbn_issn, @pub_date);

  @title = extract_title($record_xml);

  push @authors, extract_personal_authors($record_xml);

  push @authors, extract_corporate_authors($record_xml);

  push @authors, extract_meeting_authors($record_xml);

  push @authors, extract_uniform_authors($record_xml);

  @prices = extract_prices($record_xml);

  @subject_headings = extract_subject_headings($record_xml);

  @isbn_issn = extract_isbn_issn($record_xml);

  @pub_date = extract_pub_date($record_xml);

  return (\@title, \@authors, \@prices, \@subject_headings, \@isbn_issn, \@pub_date);
}

sub extract_marc_text_content {
  #pass in a space separated list of subfield to match and exlclude

  my ($marc_field, $subfield_codes_to_match, $subfield_codes_to_exclude, $record_xml) = @_;

  my @marc_field_content = ();

  my $field_xpath = 'marcxml:record/marcxml:datafield[@tag="' . $marc_field . '"]';
  
  my $subfield_xpath = create_subfield_xpath($marc_field, $subfield_codes_to_match, $subfield_codes_to_exclude);

  my @marc_field_data = $xpc->findnodes($field_xpath, $record_xml);

  my %text_content = ();

  foreach my $field_data (@marc_field_data) {

    my @subfields = $xpc->findnodes($subfield_xpath, $field_data);

    my $subfield_code;
    foreach my $subfield (@subfields) {
      my @attr_list = $subfield->attributes;
      $subfield_code = $attr_list[0]->value;
      if (!$text_content{$subfield_code}) {
        $text_content{$subfield_code} = ();
      }
      push @{$text_content{$subfield_code}}, $subfield->textContent;
    }
    
    my $final_text_content = '{';

    foreach my $key (keys %text_content) {
      my $content = "@{$text_content{$key}}";
      #escape special characters.  We need to escape
      #with six backslashes, so three are send to the
      #json parser.  It will use the first of the three
      #to escape the second.  When the string is output by
      #the json parser, the second will escape the third, which
      #will preserve a single backslash in the JSON code, which
      #is what we need to escape these special characters.
      $content =~ s/\\/\\\\/g;
      $content =~ s/"/\\\\\\"/g;
      $content =~ s/\\([xbfnrtv0])/\\\\\\$1/g;
      if ($final_text_content eq '{') {
        $final_text_content .= "\"$key\":\"$content\"";
      } else {
        $final_text_content .= ",\"$key\":\"$content\"";
      }
    }

    $final_text_content .= '}';

    $final_text_content = encode('UTF-8', $final_text_content, Encode::FB_CROAK);
  
    #If we have a marc field without the specified match subfields
    #and we have not included any exculde subfields, then we
    #will get an empty array, so exclude it from the content returned
    if ($final_text_content ne '[]') {
      push @marc_field_content, $final_text_content;
    }
    
    %text_content = ();
  }

  return @marc_field_content;
}

sub output_records {

  my $fh = shift @_;
  my $geo_json_fh = shift @_;

  my ($library_ou_id, $library_name, $library_geo_location, $lending_count, $data_creation_date, $record, $title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date, $first_feature, $average_price_per_library, $average_lending_per_work) = @_;

  output_geo_json_feature($geo_json_fh, @_);

  print $fh "$library_ou_id, $library_name\t$library_geo_location\t";
  
  my $title_data = output_array_data(@$title);

  print $fh "$title_data\t";
 
  my $author_data = output_array_data(@$authors);

  print $fh "$author_data\t";
 
  my $price_data = output_array_data(@$prices);

  print $fh "$price_data\t";

  my $subject_heading_data = output_array_data(@$subject_headings);
    
  print $fh "$subject_heading_data\t";

  my $isbn_issn_data = output_array_data(@$isbn_issn);

  print $fh "$isbn_issn_data\t";
 
  my $pub_date_data = output_array_data(@$pub_date);

  print $fh "$pub_date_data\n";
}

sub output_geo_json_header {
  my $fh = shift @_;

  print $fh '
{ "type": "FeatureCollection", 
  "features": [';
}

sub output_geo_json_footer {
  my $fh = shift @_;

  print $fh "
  ]\n";
  print $fh "}\n";
}

sub output_geo_json_feature {
  my ($fh, $library_ou_id, $library_name, $library_geo_location, $lending_count, $data_creation_date, $record, $title, $authors, $prices, $subject_headings, $isbn_issn, $pub_date, $first_feature, $average_price_per_library, $average_lending_per_work) = @_;

  my $feature_properties = '
      "properties": {
        ';

  if ($library_name) {
    my $library_name_key = 'library_name';

    my $library_name_data = create_json_data(('{"' . $library_name_key . '": "' . $library_name . '"}'));
  
    $feature_properties = append_array_to_feature_properties($library_name_data, $feature_properties, $library_name_key);
  }

  if (@$title) {
    my $title_data = create_json_data(@$title);

    $feature_properties = append_array_to_feature_properties($title_data, $feature_properties, 'title');
  }

  if (@$authors) { 
    my $author_data = create_json_data(@$authors);

    $feature_properties = append_array_to_feature_properties($author_data, $feature_properties, 'author');
  }
 
  if (@$prices) {
    my $price_data = create_json_data(@$prices);

    $feature_properties = append_array_to_feature_properties($price_data, $feature_properties, 'price');
  }

  if (@$subject_headings) {
    my $subject_heading_data = create_json_data(@$subject_headings);

    $feature_properties = append_array_to_feature_properties($subject_heading_data, $feature_properties, 'subject_heading');
  }

  if (@$isbn_issn) {
    my $isbn_issn_data = create_json_data(@$isbn_issn);

    $feature_properties = append_array_to_feature_properties($isbn_issn_data, $feature_properties, 'isbn_issn');
  }

  if (@$pub_date) {
    my $pub_date_data = create_json_data(@$pub_date);

    $feature_properties = append_array_to_feature_properties($pub_date_data, $feature_properties, 'pub_date');
  }

  if ($lending_count) {
    my $lending_count_key = 'single_year_lending_count';

    my $lending_count_data = create_json_data(('{"' . $lending_count_key . '": "' . $lending_count . '"}'));

    $feature_properties = append_array_to_feature_properties($lending_count_data, $feature_properties, $lending_count_key);
  }

  if ($record) {
    my $record_id_key = 'sitka_record_id';

    my $record_id_data = create_json_data(('{"' . $record_id_key . '": "' . $record . '"}'));

    $feature_properties = append_array_to_feature_properties($record_id_data, $feature_properties, $record_id_key);
  }

  if ($data_creation_date) {
    my $data_creation_date_key = 'data_creation_date';
    
    my $data_creation_date_data = create_json_data(('{"' . $data_creation_date_key . '": "' . $data_creation_date . '"}'));

    $feature_properties = append_array_to_feature_properties($data_creation_date_data, $feature_properties, $data_creation_date_key);
  }

  if ($average_price_per_library) {
    my $average_price_per_library_key = 'average_price_per_library';

    my $average_price_per_library_data = create_json_data(('{"' . $average_price_per_library_key . '": "' . $average_price_per_library . '"}'));

    $feature_properties = append_array_to_feature_properties($average_price_per_library_data, $feature_properties, $average_price_per_library_key);
  }

  if ($average_lending_per_work) {
    my $average_lending_per_work_key = 'average_lending_per_work';

    my $average_lending_per_work_data = create_json_data(('{"' . $average_lending_per_work_key . '": "' . $average_lending_per_work . '"}'));

    $feature_properties = append_array_to_feature_properties($average_lending_per_work_data, $feature_properties, $average_lending_per_work_key);
  }

  $feature_properties .= '
      }';

  my $feature = create_geo_json_feature($fh, $library_ou_id, $library_name, $library_geo_location, $feature_properties, $first_feature);

  print $fh $feature;
}

sub append_array_to_feature_properties {
  my ($array, $feature_properties, $feature_key) = @_;

  my $index = 0;

  foreach my $data (@$array) {
    my $combined_data = '';
    foreach my $key (keys %$data) {
      $combined_data .= ($data->{$key} ? "$data->{$key}" : '');
    }

    #Only create a property if there is data in the MARC subfield
    if ($combined_data) {
      #The indentation is messy to generate pretty output
      if ($feature_properties ne '
      "properties": {
        ') {
        $feature_properties .= ',
        ';
      }

      $feature_properties .= create_feature_property("$feature_key$index", $combined_data);
      my $test = create_feature_property("$feature_key$index", $combined_data);

      $index++;
    }
  }

  return $feature_properties;
}

sub create_json_data {
  my (@array_of_data) = @_;
  return decode_json '[' . join(',', @array_of_data) . ']';
}

sub create_feature_property {
  my ($property_key, $property_value) = @_;

  return '"' . $property_key . '": "' . $property_value . '"';
}

sub create_geo_json_feature {
  my ($fh, $library_ou_id, $library_name, $library_geo_location, $feature_properties, $first_feature) = @_;

  my $geo_json_feature = '';

  if (!$first_feature) {
    $geo_json_feature = ',';
  }

  $geo_json_feature .= '
    { 
      "type": "Feature",
      "id": "' . $library_ou_id . '",
      "geometry": { 
        "type": "Point", 
        "coordinates": [' . $library_geo_location . '] 
        },'
        . $feature_properties . '
    }';

  return $geo_json_feature;
}

sub output_array_data {
  my @array = @_;
  my $data = '{';

  foreach my $value (@array) {
    if ($data eq '{') {
      $data .= $value;
    } else {
      $data .= ",$value";
    }
  }

  $data .= '}';

  return $data;
}

sub create_subfield_xpath {
 
  my ($marc_field, $subfield_codes_to_match, $subfield_codes_to_exclude) = @_;

  my $subfield_xpath = "";

  if (@$subfield_codes_to_match == 1) {
    $subfield_xpath = 'marcxml:subfield[@code="' . "@$subfield_codes_to_match";
  } elsif (@$subfield_codes_to_match > 1) {
    $subfield_xpath = 'marcxml:subfield[@code="' . join('" or @code="', split(' ', "@$subfield_codes_to_match"));
  }

  if (@$subfield_codes_to_exclude) {
    if (!$subfield_xpath) {
      #we have not populated $subfield_xpath yet, so set it up to start with exclusion
      $subfield_xpath = 'marcxml:subfield[@code!="';
    } else {
      $subfield_xpath .= '" or @code!="';
    }
    
    if (@$subfield_codes_to_exclude == 1) {
      $subfield_xpath .= "@$subfield_codes_to_exclude";
    } else {
      $subfield_xpath .= join('" or @code!="', split(' ', "@$subfield_codes_to_exclude"));
    }
  } elsif (!@$subfield_codes_to_match) {
    #we do not have excludes and we do not have matches, this cannot happen
    die "You must specify a subfield match or exclude list for field $marc_field\n";
  }

  return $subfield_xpath .= '"]';
}

sub extract_title {
  my ($record_xml) = @_;
  return extract_field_data(['245'], ['a', 'b'], [], $record_xml);
}

sub extract_personal_authors {
  my ($record_xml) = @_;
  return extract_field_data(['100'], ['a'], [], $record_xml);
}

sub extract_corporate_authors {
  my ($record_xml) = @_;
  return extract_field_data(['110'], ['a', 'b'], [], $record_xml);
}

sub extract_meeting_authors {
  my ($record_xml) = @_;
  return extract_field_data(['111'], ['a'], [], $record_xml);
}

sub extract_uniform_authors {
  my ($record_xml) = @_;
  return extract_field_data(['130'], ['a'], [], $record_xml);
}

sub extract_prices {
  my ($record_xml) = @_;
  return extract_field_data(['020'], ['c'], [], $record_xml);
}

sub extract_subject_headings {
  my ($record_xml) = @_;
  return extract_field_data(['600', '610', '611', '630', '648', '650', '651', '654', '662'], [], [0], $record_xml);
}

sub extract_isbn_issn {
  my ($record_xml) = @_;
  return extract_field_data(['020'], ['a'], [], $record_xml);
}

sub extract_pub_date {
  my ($record_xml) = @_;
  return extract_field_data(['260'], ['c'], [], $record_xml);
}

sub extract_field_data {
  my ($fields, $subfield_match, $subfield_exclude, $record_xml) = @_;
  
  my @field_content;
  
  foreach my $field (@$fields) {
    my @test = extract_marc_text_content($field, $subfield_match, $subfield_exclude, $record_xml);
    push @field_content, extract_marc_text_content($field, $subfield_match, $subfield_exclude, $record_xml);
  }

  return @field_content;
}

sub fix_geo_location_data {
  #the geolocation data in the input files is incorrect. It has transposed lat and longs, 
  #as well the longatudes are positive when they should be negative
 
  my ($geo_location) = @_;

  my $latitude = $geo_location;

  my $longitude = $geo_location;

  $latitude =~ s/^([0-9][0-9.]*).*$/$1/;

  $longitude =~ s/^.*,([0-9][0-9.]*)$/$1/;

  $longitude = '-' . $longitude;

  return $longitude . ", " . $latitude; 
}

sub extract_price {
  my ($prices) = @_;

  my @copy_of_prices = @$prices;

  my $price_re = qr/^[\D]*(\d\d*.\d\d*|.\d\d*|\d\d*)[\D]*$/;

  my $price = 0;

  my $least_expensive = 0;

  for $price (@copy_of_prices) {
    $price =~ s/$price_re/$1/;

    if ($price =~ m/[^\d.][^\d.]*/) {
      $price = 0;
    }

    if ($least_expensive == 0) {
      $least_expensive = $price;
    } else {
      if ($price < $least_expensive && $price != 0) {
        $least_expensive = $price;
      }
    }
  }

  return $least_expensive;
}

sub average_prices {

  my (@all_prices) = @_;

  my $sum = 0;

  my $price_count = 0;

  for my $price (@all_prices) {

    if ($price) {
      $price_count++;

      if ($price > 500) {
        #exclude books over $500
        #some of the averages were in the
        #10,000s or higher, so some of the
        #price data must be wrong. $500 should be
        #significantly large enough to keep most
        #relevant data
        $price = 0;
        $price_count--;
      }
    }

    $sum += $price;
  }

  my $average_price = 0;
  
  if ($price_count) {
    $average_price = $sum / $price_count;
  }

  $average_price =~ s/^(\d\d*.\d\d)\d*$/$1/;

  if ($average_price =~ m/^\d\d*$/) {
    $average_price .= '.00';
  }

  return $average_price;
}

sub average_lends {
 
  my @all_lends = shift @_;
  my $work_count = shift @_;

  my $sum = 0;

  for my $lend_count (@all_lends) {
    $sum += $lend_count;
  }

  my $average_lends = $sum / $work_count;

  return $average_lends;
}

