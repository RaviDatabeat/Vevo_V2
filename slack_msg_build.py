import pandas as pd

from utils import name_shortner


def outer_user_block(user_id: str):
    return {
        "type": "rich_text_list",
        "style": "bullet",
        "indent": 0,
        "border": 0,
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {"type": "user", "user_id": user_id},
                    # {
                    #     "type": "text",
                    #     "text": r" : line items below require action due to a high % of vast errors",
                    # },
                ],
            }
        ],
    }


def outer_user_text_block(user_email: str):
    return {
        "type": "rich_text_list",
        "style": "bullet",
        "indent": 0,
        "border": 0,
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {"type": "text", "text": user_email},
                    # {
                    #     "type": "text",
                    #     "text": r" : line items below require action due to a high % of vast errors",
                    # },
                ],
            }
        ],
    }


def inner_info_block(grouped_df: pd.DataFrame):
    elements = []
    grouped_df.sort_values(
        "line_item_id", inplace=True, ascending=False
    )
    for j in grouped_df.itertuples():
        elements.append(
            {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "link",
                        "url": f"https://admanager.google.com/40576787#delivery/line_item/detail/line_item_id={j.line_item_id}&li_tab=settings",
                        "text": name_shortner(f"{j.line_item_name} | {j.creative_size}"),
 # type: ignore
                    },
                    {
                        "type": "text",
                        "text": f" : Creative Size = {j.creative_size}",
                    },

                ],
            },
        )

    return {
        "type": "rich_text_list",
        "style": "ordered",
        "indent": 1,
        "border": 0,
        "elements": elements,
    }
