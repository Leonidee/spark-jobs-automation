import warnings


def main():
    # d = dict(
    #     iamToken="14902934", status="success", login="lgrishenkov", dt="2023-01-01"
    # )
    d = {
        # "    IamTOkeN  ": "14902934",
        " sTAtus": "success",
        "  Login    ": "lgrishenkov",
        "Dt ": "2023-01-01",
    }

    # if "iamToken" in d.keys():
    #     print("yes")
    # print({x.lower().strip() for x in d.keys()})

    # if "iamtoken" in {x.lower().strip() for x in d.keys()}:
    #     print(True)
    import re

    # for _ in d.keys():
    #     token_name = _ if re.search("iamtoken", _, re.IGNORECASE) else "iamToken"

    # for _ in d.keys():
    try:
        token_name = next(
            _ for _ in d.keys() if re.search("iamtoken", _, re.IGNORECASE)
        )
        print(token_name)
        print(d[token_name])
        print(type(token_name))
    except StopIteration:
        warnings.warn("Some warning. Are you sure?", UserWarning)


if __name__ == "__main__":
    main()
