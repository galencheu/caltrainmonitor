# Caltrain Monitor 🚂

This project was forked from https://github.com/tyler-simons/caltrain/tree/main with substantial customisations to fit on a home monitor. For the past year I have had Caltrain schedules posted on the wall, but when I get to the station I see the train is delayed. I wanted to be able to run something on my raspi and mini 8" monitor to let me know the status of the trains. There was no good out of the box solution, but Tyler's app was the closest. I have done modifications to it primarly related to how much data is displayed (i need it to be compact for my small monitor) and updates to reflect current API results and train numbering. Since this is a static monitor, I also have it set to just be on the "Origin" to know when I need to leave.

## Usage

This is a local streamlit app to run on a raspi/home monitor.

## Installation

To get started with the project, you will need to clone this repository and install the requirements listed in the `requirements.txt` file in the base directory folder. Here's how to do it:

1. Clone the repository
2. Install the requirements: `pip install -r requirements.txt`

Once you have installed the requirements, you can play around with the script locally.
`streamlit run stcaltrain.py`
