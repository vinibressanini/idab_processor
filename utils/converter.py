class Converter():

    def cast(data, type):

        match type:
            case 'float':
                return float(data)