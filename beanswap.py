import os
import pandas as pd
from random import shuffle

__author__ = "Patrick Crutchley"
__version__ = "0.0.1"
__license__ = "MIT"

import argparse


def parse_data(df):

    required_cols = [
        "username",
        "mailing_address",
        "bean_preferences",
        "how_many_people",
        "receiving_home-roast",
        "send_home-roast",
        "contact_preference",
        "email_address",
    ]

    rename_mapper = dict()
    for newcol in required_cols:
        oldcol = [i for i in df.columns if newcol.replace("_", " ") in i.lower()]
        if len(oldcol) > 1:
            raise (
                ValueError,
                f"No more than one column can match {newcol.replace('_', ' ')}",
            )
        elif len(oldcol) == 0:
            pass
        else:
            rename_mapper[oldcol[0]] = newcol

    df = df.rename(rename_mapper, axis="columns")

    return df


def perform_matching(df):
    # randomize, check for self-matches, and non-unique pairs

    repeat_list = []
    for _, row in df.iterrows():
        repeat_list += [row["username"]] * row["how_many_people"]

    match_df = pd.DataFrame(columns=["sender", "receiver"])
    match_df["sender"] = repeat_list

    done = False
    while not done:
        shuffle(repeat_list)
        match_df["receiver"] = repeat_list
        if any(match_df["sender"] == match_df["receiver"]):
            # try again if any self-assigned
            continue
        if any(match_df.duplicated()):
            # try again if any matches happened more than once
            continue

        # otherwise should be good
        done = True

    return match_df


def format_output(args, match_df, clean_df):
    match_df = match_df.join(clean_df.set_index("username"), on="receiver")
    messages = []
    for _, row in clean_df.sort_values("contact_preference").iterrows():
        matches = match_df[match_df["sender"] == row["username"]]

        message = f"""
Send to: {row['username']}
Via: {row['contact_preference']}
Email: {row['email_address']}

----
Hi {row['username']},

Thanks for bearing with me on this. Busy time of year! I've randomly assigned you the {f"{row['how_many_people']} " if row['how_many_people']>1 else ''}following match{'es' if row['how_many_people']>1 else ''}:

        """
        for _, match in matches.iterrows():
            message += f"""
Name/username: {match['receiver']}
Address: {match['mailing_address']}
Bean preferences: {match['bean_preferences']}

            """
        message += """
I suggest sending via USPS first-class in a padded mailer via pirateship.com; I think I paid less than $4 to send 3 oz of beans across the country in 3 days recently. Don't forget to include info about the beans!

Let me (PatrickC) know if you have any questions. Post in or refer back to the forum post as needed! https://decentforum.com/t/us-secret-krampus-freezer-swap-give-a-bean-get-a-bean/3541

Please send by January 15, 2022. Feel free to post in the thread when/what you send/receive.

Quick-and-dirty code for the matching and message generation at https://github.com/pcrutchl/beanswap

Happy brew-lidays!

Patrick

----

        """
        with open(args.output, "a+") as f:
            f.write(message)
        messages.append(message)
    return messages


def main():
    parser = argparse.ArgumentParser()

    # Required positional argument
    parser.add_argument("filename", help="data CSV")

    parser.add_argument("-o", "--output", help="output text file", default="output.txt")

    parser.add_argument(
        "--clobber",
        help="overwrite output file if exists",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s (version {__version__})",
    )

    args = parser.parse_args()

    if os.path.exists(args.output):
        if not args.clobber:
            raise FileExistsError("output file exists already and clobber is off")
        else:
            os.remove(args.output)

    input_df = pd.read_csv(args.filename)
    clean_df = parse_data(input_df)
    match_df = perform_matching(clean_df)

    messages = format_output(args, match_df, clean_df)
    for message in messages:
        print(message)


if __name__ == "__main__":
    main()
