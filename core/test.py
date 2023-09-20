import datetime

if __name__ == '__main__':
    metadata={
        "A":1,
        "B":1,
        "C":1,
        "D":"eqwefsdfdgbdfgdfgdgd",
    }
    new_dict = {key: metadata[key] if key!='D' else metadata[key][0:3] for key in metadata.keys()}
    print(new_dict)
