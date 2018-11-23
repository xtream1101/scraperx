import random
from ast import literal_eval

user_agents = {"desktop":["('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36', 0.27941176470588236)","('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36', 0.1642156862745098)","('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36', 0.11274509803921566)","('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36', 0.10539215686274508)","('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36', 0.07352941176470587)","('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38', 0.07107843137254902)","('Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36', 0.06862745098039215)","('Mozilla/5.0 (Windows NT 10.0; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0', 0.051470588235294115)","('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063', 0.0392156862745098)","('Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko', 0.034313725490196074)"],"mobile": ["('Mozilla/5.0 (Linux; Android 7.0; SM-G955U Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.81 Mobile Safari/537.36', 0.64)","('Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile Safari/602.1', 0.11)","('Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/603.1.23 (KHTML, like Gecko) Version/10.0 Mobile Safari/602.1', 0.25)"]}  # noqa


def get_user_agent(device_type='desktop'):
    """Get a random desktop or mobile user agent string

    Keyword Arguments:
        device_type {str} -- The type of UA to get (default: {'desktop'})

    Returns:
        str -- The choosen random UA

    Raises:
        ValueError -- if device_type is not in the supported list
    """
    if device_type is None:
        # This extra check is here to make sure the default is really desktop
        device_type = 'desktop'

    device_type = device_type.lower()
    if device_type not in user_agents:
        raise ValueError(f"{device_type}: invalid device type")

    device_uas = [literal_eval(ua_tuple)
                  for ua_tuple in user_agents[device_type]]

    user_agent_list, probabilities = zip(*device_uas)
    user_agent = random.choices(user_agent_list, probabilities)[0]

    return user_agent
