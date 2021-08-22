# Hello there, Datawan!

Last night a bored cat entered our data processing server room and wrecked havoc, causing our servers to overheat and die (the cat managed to escape on time, though). Due to this unfortunate incident, we have lost our script that we use to process certain set of files. Therefore we need your help to regain our script and not keep customers waiting.

In the 'Ocean Freight Rates' sheet, you will see dummy values of ocean freight rates. Details of the information you see in that sheet is provided at the end of this explanation.

In the 'Lookups' sheet, you will see a mapping of port names and port codes. This information is also provided separately in a SQLite database file.
It will be up to you to decide whether you would like to utilize the database version or the Lookups sheet. Of course, pulling this information from the xeneta.db file will take more effort compared to pulling it from Lookups sheet, and get you extra points on your test.

For the entirety of the test, you are not allowed to modify the input file. Your code should have the file as pure input without any prior modifications.

## Main task:
Your goal is to convert the data you see in the 'Ocean Freight Rates' sheet to the format specified by the headers in 'Final Import' sheet. You will face multiple challenges along the way and it will be up to your imagination to overcome these challenges. Provide us with a script that takes the file as the input and provides an output file in Excel format (.xlsx). If you have additional findings along the way, you may include them in your script or you may provide them in a separate file. At the end, make sure that you compress all documents and put them in a '*name_lastname*.zip', then send it to us over email.

We remember the customers complaining about their 'THC Used' values being wrong. Upon further investigation, we have noticed that the customer sends us an incomplete file. This implies that the 'Inclusive Surcharges' column does not always show whether OTHC and DTHC values are included in the rates correctly. Our previous script corrected this for us, but since we do not have it right now, you will need to provide us also this functionality in the new script. This problem was found to only affect the OHC and DHC values and not the rest of the surcharges.
- In case OHC/DHC values are given as 0, you should assume that they are forgotten to be included in the 'Inclusive Surcharges' column. This affects calculation of 'THC Used'.

Throughout this test, "Charge" and "Surcharge" terms are used interchangably.

## Acceptance criteria for the 'Final Import' sheet:
- Port codes should match the port_code column in the Lookups sheet
- You should not have duplicate rows.
- Your solution should only include rates for "FAK" commodity and "CY/CY" service mode.
- Your solution should only include Charges that have 'PER_CONTAINER' rate basis.
- You should calculate "THC Used" according to the following criteria:
    - If the Inclusive Surcharges include 'OHC' and not 'DHC', 'THC Used' should be 'OTHC'
    - If the Inclusive Surcharges include 'DHC' and not 'OHC', it should be 'DTHC'
    - If the Inclusive Surcharges include both 'OHC' and 'DHC' in it, it should be 'BOTH'
    - If the Inclusive Surcharges do not include either 'OHC' or 'DHC', it should be 'NONE'
- Prior to row 7 of Ocean Freight Rates sheet you will see certain information. This information is the same for all of the rows in the Ocean Freight Rates, therefore should be treated as such in the Final Import sheet.
- Receipt and Load Port columns are used to decide the Origin Port of the rate. If there is Load Port value, this should be prioritized as the Origin Port.
- Delivery and Discharge Port columns are used to decide the Destination Port of the rate. If there is Discharge Port value, this should be prioritized as the Destination Port.
- You should exclude surcharges with 0 value. They should show up as NaN (Python) or NA (R) instead of 0. You should keep the columns for all surcharges as long as there is at least 1 rate/datapoint/observation with a value for that surcharge. If none of the datapoints have any non-zero price for that surcharge, you should drop the surcharge altogether.
- You should exclude 45HDRY equipment type.
- You should rename the equipment types to the following names:
    * 20DRY should become 20DC,
    * 40DRY should become 40DC,
    * 40HDRY should become 40HC.

## Details of 'Ocean Freight Rates':
- Effective Date and Expiry Date stands for when the contract begins to be valid and at what date the contract will stop being valid. (both dates are inclusive)
- Service Mode denotes the nature of Origin and Destination ports/locations. CY stands for Container Yard and you should make sure both the origin and the destination is a container yard.
- Commodity Name denotes for which commodity the price is valid. 'FAK' stands for 'Freight of All Kinds'
- Inclusive Surcharges show which surcharges are included in the Basic Freight Rate.

## Explanation of surcharges:
- BAS: Basic Freight Rate. This is usually the most expensive charge and implies the base ocean freight rate.
- CFD: Congestion Fee at Destination. This is a surcharge that is caused by the destination port.
- CFO: Congestion Fee at Origin. This is a surcharge that is caused by the origin port.
- DDF: Documentation fee at Destination.
- DHC: Also referred to as DTHC. This is terminal handling service fee at Destination.
- DPA: Destination Port Additionals.
- EBS: Emergency Bunker Fee. This is a fuel-related surcharge that is related to neither Origin nor Destination.
- ERS: Emergency Risk Surcahrge. This is often a security surcharge that is related to either Origin or Destination.
- EXP: Export Service Fee. This surcharge is related to Origin.
- IMP: Import Service Fee. This surcharge is related to Destination.
- LSS: Low Sulphur Surcharge. This surcharge is related to neither Origin nor Destination.
- ODF: Documentation fee at Origin.
- OHC: Also referred to as OTHC. This is terminal handling service fee at Origin.
- OPA: Origin Port Additionals
- PAE: Port Dues Export. This surcharge consists of fees paid at Origin Port.
- PSS: Peak Season Surcharge. This surcharge is not related to Origin or Destination.
- RHI: Recovery for Handling at Import. This is a surcharge related to Destination port.
- SBF: Standard Bunker Adjustment Factor. This is a fuel related surcharge that is not related to Origin or Destination ports.

## Other guidelines:
- You may use Python or R to solve this test. Python is our preferred language, but we will accept R solutions, too.
- Make sure that your code is understandable, provide comments.
- Please provide the Excel output of your script in your submission (output.xlsx).
- If you're stuck at some point, make an assumption and solve according to your assumption.
- State all of your assumptions carefully and in detail. Without your assumptions, your solution may be interpreted incorrectly.
- If you have additional findings, feel free to include them with your result. You may provide us with additional files besides the script and the output.

Be curious and be judgemental.

Best of luck!

