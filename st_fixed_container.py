from typing import Literal
import streamlit as st
from streamlit.components.v1 import html

"""
st_fixed_container consist of two parts - fixed container and opaque container.
Fixed container is a container that is fixed to the top or bottom of the screen.
When transparent is set to True, the container is typical `st.container`, which is transparent by default.
When transparent is set to False, the container is custom opaque_container, that updates its background color to match the background color of the app.
Opaque container is a helper class, but can be used to create more custom views. See main for examples.
"""
OPAQUE_CONTAINER_CSS = """
:root {{
    --background-color: #ffffff; /* Default background color */
}}
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.opaque-container-{id}):not(:has(div.not-opaque-container)) div[data-testid="stVerticalBlock"]:has(div.opaque-container-{id}):not(:has(div.not-opaque-container)) > div[data-testid="stVerticalBlockBorderWrapper"] {{
    background-color: var(--background-color);
    width: 100%;
}}
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.opaque-container-{id}):not(:has(div.not-opaque-container)) div[data-testid="stVerticalBlock"]:has(div.opaque-container-{id}):not(:has(div.not-opaque-container)) > div[data-testid="element-container"] {{
    display: none;
}}
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.not-opaque-container):not(:has(div[class^='opaque-container-'])) {{
    display: none;
}}
""".strip()

OPAQUE_CONTAINER_JS = """
const root = parent.document.querySelector('.stApp');
let lastBackgroundColor = null;
function updateContainerBackground(currentBackground) {
    parent.document.documentElement.style.setProperty('--background-color', currentBackground);
    ;
}
function checkForBackgroundColorChange() {
    const style = window.getComputedStyle(root);
    const currentBackgroundColor = style.backgroundColor;
    if (currentBackgroundColor !== lastBackgroundColor) {
        lastBackgroundColor = currentBackgroundColor; // Update the last known value
        updateContainerBackground(lastBackgroundColor);
    }
}
const observerCallback = (mutationsList, observer) => {
    for(let mutation of mutationsList) {
        if (mutation.type === 'attributes' && (mutation.attributeName === 'class' || mutation.attributeName === 'style')) {
            checkForBackgroundColorChange();
        }
    }
};
const main = () => {
    checkForBackgroundColorChange();
    const observer = new MutationObserver(observerCallback);
    observer.observe(root, { attributes: true, childList: false, subtree: false });
}
// main();
document.addEventListener("DOMContentLoaded", main);
""".strip()


def st_opaque_container(
    *,
    height: int | None = None,
    border: bool | None = None,
    key: str | None = None,
):
    global opaque_counter

    opaque_container = st.container()
    non_opaque_container = st.container()
    css = OPAQUE_CONTAINER_CSS.format(id=key)
    with opaque_container:
        html(f"<script>{OPAQUE_CONTAINER_JS}</script>", scrolling=False, height=0)
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='opaque-container-{key}'></div>",
            unsafe_allow_html=True,
        )
    with non_opaque_container:
        st.markdown(
            f"<div class='not-opaque-container'></div>",
            unsafe_allow_html=True,
        )

    return opaque_container.container(height=height, border=border)


FIXED_CONTAINER_CSS = """
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.fixed-container-{id}):not(:has(div.not-fixed-container)){{
    background-color: transparent;
    position: {mode};
    width: inherit;
    background-color: inherit;
    {position}: {margin};
    z-index: 999;
}}
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.fixed-container-{id}):not(:has(div.not-fixed-container)) div[data-testid="stVerticalBlock"]:has(div.fixed-container-{id}):not(:has(div.not-fixed-container)) > div[data-testid="element-container"] {{
    display: none;
}}
div[data-testid="stVerticalBlockBorderWrapper"]:has(div.not-fixed-container):not(:has(div[class^='fixed-container-'])) {{
    display: none;
}}
""".strip()

MARGINS = {
    "top": "2.875rem",
    "bottom": "0"
}


def st_fixed_container(
    *,
    height: int | None = None,
    border: bool | None = None,
    mode: Literal["fixed", "sticky"] = "fixed",
    position: Literal["top", "bottom","right"] = "top",
    margin: str | None = None,
    transparent: bool = False,
    key: str | None = None,
):
    if margin is None:
        margin = MARGINS[position]
    global fixed_counter
    fixed_container = st.container()
    non_fixed_container = st.container()
    css = FIXED_CONTAINER_CSS.format(
        mode=mode,
        position=position,
        margin=margin,
        id=key,
    )
    with fixed_container:
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='fixed-container-{key}'></div>",
            unsafe_allow_html=True,
        )
    with non_fixed_container:
        st.markdown(
            f"<div class='not-fixed-container'></div>",
            unsafe_allow_html=True,
        )

    with fixed_container:
        if transparent:
            return st.container(height=height, border=border)

        return st_opaque_container(height=height, border=border, key=f"opaque_{key}")


if __name__ == "__main__":
    for i in range(30):
        st.write(f"Line {i}")

    # with st_fixed_container(mode="sticky", position="bottom", border=True):
    # with st_fixed_container(mode="sticky", position="top", border=True):
    # with st_fixed_container(mode="fixed", position="bottom", border=True):
    with st_fixed_container(mode="fixed", position="top", border=True):
        st.write("This is a fixed container.")
        st.write("This is a fixed container.")
        st.write("This is a fixed container.")

    # The following code creates a small control panel on the right side of the screen with two buttons inside it:
    with st_fixed_container(mode="fixed", position="bottom", transparent=True):
        _, right = st.columns([0.7, 0.3])
        with right:
            with st_opaque_container(border=True):
                st.button("Feedback", use_container_width=True)
                st.button("Clean up", use_container_width=True)

    st.container(border=True).write("This is a regular container.")
    for i in range(30):
        st.write(f"Line {i}")
