import re

# Regex's compile once to be reused on each fn call
rating_pattern = re.compile(r'(?:5[^.,])?.*?(\d(?:[.,]\d+)?).*5?')
price_pattern = re.compile(r'(?P<low>[\d,.]+)(?:\D*(?P<high>[\d,.]+))?')


def rating(rating_str):
    """Parse the rating from a string using a regex

    Testing & debugging are here: https://regex101.com/r/ChmgmF/4

    Args:
        rating_str (str): String that needs to be parsed.

    Returns:
        float: Rating as a float
    """
    rating = None
    if rating_str is not None:
        m = re.search(rating_pattern, rating_str)
        if m is not None:
            rating = float(m.group(1).replace(',', '.'))

    return rating


def price(price_str):
    """Parse the price(s) out of a string using a regex

    Args:
        price_str (str): String of the price to parse out.

    Returns:
        dict: Contains the `low` price and the `high` price as floats.
            Just `low` is set in the case of a single price.
    """
    found_price = {'low': None,
                   'high': None
                   }
    price_raw = re.search(price_pattern, price_str)
    if price_raw:
        matched = price_raw.groupdict()
        found_price['low'] = matched.get('low')
        found_price['high'] = matched.get('high')

    for key, value in found_price.items():
        if value is not None:
            value = value.strip()
            new_value = value.replace(',', '').replace('.', '').replace(' ', '')
            try:
                # Check if price has cents
                if value[-3] in [',', '.']:
                    # Add . for cents back
                    new_value = new_value[:-2] + '.' + new_value[-2:]
            except IndexError:
                # Price is 99 or less with no cents
                pass

            if new_value != '':
                found_price[key] = float(new_value)
            else:
                found_price[key] = None

    return found_price
