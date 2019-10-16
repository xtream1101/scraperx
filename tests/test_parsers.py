from scraperx import parsers


def test_rating_chinese():
    assert parsers.rating('平均 4.9 星') == 4.9


def test_rating_japanese():
    assert parsers.rating('5つ星のうち 3.9') == 3.9


def test_rating_no_out_of():
    assert parsers.rating('An average of 4.1 star') == 4.1


def test_rating_float_out_of():
    assert parsers.rating('4.4 out of 5 stars') == 4.4


def test_rating_int_out_of():
    assert parsers.rating('3 out of 5 stars') == 3


def test_rating_5_out_of_5():
    assert parsers.rating('5.0 out of 5 stars') == 5


def test_rating_with_comma_as_dec():
    assert parsers.rating('5,0 von 5 Sternen') == 5


def test_rating_text_first():
    assert parsers.rating('Rated 4.82 out of 5 stars') == 4.82


def test_price_basic_low_only():
    assert parsers.price('$47.50') == {'low': 47.5, 'high': None}


def test_price_basic_low_high():
    assert parsers.price('$23 - $27.89') == {'low': 23, 'high': 27.89}


def test_price_low_in_string_first():
    assert parsers.price('$1.10 off') == {'low': 1.1, 'high': None}


def test_price_low_in_string_last():
    assert parsers.price('Save $1.00') == {'low': 1.0, 'high': None}
