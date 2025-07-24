## FCS Landcover (30m) Legend

    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 20px;
            background-color: #f8f9fa;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th {
            background-color: #f1f3f4;
            color: #333;
            font-weight: 600;
            padding: 12px;
            border: 1px solid #ddd;
            text-align: left;
        }
        td {
            padding: 10px 12px;
            border: 1px solid #ddd;
            vertical-align: middle;
        }
        .quantity {
            font-weight: 600;
            text-align: center;
            width: 80px;
        }
        .color-cell {
            width: 100px;
            text-align: center;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            font-weight: bold;
            color: white;
            text-shadow: 1px 1px 1px rgba(0,0,0,0.7);
        }
        .label {
            font-size: 14px;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .light-text {
            color: black !important;
            text-shadow: 1px 1px 1px rgba(255,255,255,0.8);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Land Cover Classification Color Map</h1>
        <table>
            <thead>
                <tr>
                    <th>Quantity</th>
                    <th>Color</th>
                    <th>Label</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td class="quantity">0</td>
                    <td class="color-cell light-text" style="background-color: #ffffff;">#ffffff</td>
                    <td class="label">Filled value (0)</td>
                </tr>
                <tr>
                    <td class="quantity">10</td>
                    <td class="color-cell" style="background-color: #ffff64; color: black;">#ffff64</td>
                    <td class="label">Rainfed cropland (10)</td>
                </tr>
                <tr>
                    <td class="quantity">11</td>
                    <td class="color-cell" style="background-color: #ffff64; color: black;">#ffff64</td>
                    <td class="label">Herbaceous cover cropland (11)</td>
                </tr>
                <tr>
                    <td class="quantity">12</td>
                    <td class="color-cell" style="background-color: #ffff00; color: black;">#ffff00</td>
                    <td class="label">Tree or shrub cover (Orchard) cropland (12)</td>
                </tr>
                <tr>
                    <td class="quantity">20</td>
                    <td class="color-cell" style="background-color: #aaf0f0; color: black;">#aaf0f0</td>
                    <td class="label">Irrigated cropland (20)</td>
                </tr>
                <tr>
                    <td class="quantity">51</td>
                    <td class="color-cell" style="background-color: #4c7300;">#4c7300</td>
                    <td class="label">Open evergreen broadleaved forest (51)</td>
                </tr>
                <tr>
                    <td class="quantity">52</td>
                    <td class="color-cell" style="background-color: #006400;">#006400</td>
                    <td class="label">Closed evergreen broadleaved forest (52)</td>
                </tr>
                <tr>
                    <td class="quantity">61</td>
                    <td class="color-cell" style="background-color: #aac800; color: black;">#aac800</td>
                    <td class="label">Open deciduous broadleaved forest (61)</td>
                </tr>
                <tr>
                    <td class="quantity">62</td>
                    <td class="color-cell" style="background-color: #00a000;">#00a000</td>
                    <td class="label">Closed deciduous broadleaved forest (62)</td>
                </tr>
                <tr>
                    <td class="quantity">71</td>
                    <td class="color-cell" style="background-color: #005000;">#005000</td>
                    <td class="label">Open evergreen needle-leaved forest (71)</td>
                </tr>
                <tr>
                    <td class="quantity">72</td>
                    <td class="color-cell" style="background-color: #003c00;">#003c00</td>
                    <td class="label">Closed evergreen needle-leaved forest (72)</td>
                </tr>
                <tr>
                    <td class="quantity">81</td>
                    <td class="color-cell" style="background-color: #286400;">#286400</td>
                    <td class="label">Open deciduous needle-leaved forest (81)</td>
                </tr>
                <tr>
                    <td class="quantity">82</td>
                    <td class="color-cell" style="background-color: #285000;">#285000</td>
                    <td class="label">Closed deciduous needle-leaved forest (fc >0.4) (82)</td>
                </tr>
                <tr>
                    <td class="quantity">91</td>
                    <td class="color-cell" style="background-color: #a0b432;">#a0b432</td>
                    <td class="label">Open mixed leaf forest (broadleaved and needle-leaved) (91)</td>
                </tr>
                <tr>
                    <td class="quantity">92</td>
                    <td class="color-cell" style="background-color: #788200;">#788200</td>
                    <td class="label">Closed mixed leaf forest (broadleaved and needle-leaved) (92)</td>
                </tr>
                <tr>
                    <td class="quantity">120</td>
                    <td class="color-cell" style="background-color: #966400;">#966400</td>
                    <td class="label">Shrubland (120)</td>
                </tr>
                <tr>
                    <td class="quantity">121</td>
                    <td class="color-cell" style="background-color: #964b00;">#964b00</td>
                    <td class="label">Evergreen shrubland (121)</td>
                </tr>
                <tr>
                    <td class="quantity">122</td>
                    <td class="color-cell" style="background-color: #966400;">#966400</td>
                    <td class="label">Deciduous shrubland (122)</td>
                </tr>
                <tr>
                    <td class="quantity">130</td>
                    <td class="color-cell" style="background-color: #ffb432; color: black;">#ffb432</td>
                    <td class="label">Grassland (130)</td>
                </tr>
                <tr>
                    <td class="quantity">140</td>
                    <td class="color-cell" style="background-color: #ffdcd2; color: black;">#ffdcd2</td>
                    <td class="label">Lichens and mosses (140)</td>
                </tr>
                <tr>
                    <td class="quantity">150</td>
                    <td class="color-cell" style="background-color: #ffebaf; color: black;">#ffebaf</td>
                    <td class="label">Sparse vegetation (150)</td>
                </tr>
                <tr>
                    <td class="quantity">152</td>
                    <td class="color-cell" style="background-color: #ffd278; color: black;">#ffd278</td>
                    <td class="label">Sparse shrubland (152)</td>
                </tr>
                <tr>
                    <td class="quantity">153</td>
                    <td class="color-cell" style="background-color: #ffebaf; color: black;">#ffebaf</td>
                    <td class="label">Sparse herbaceous (153)</td>
                </tr>
                <tr>
                    <td class="quantity">181</td>
                    <td class="color-cell" style="background-color: #00a884;">#00a884</td>
                    <td class="label">Swamp (181)</td>
                </tr>
                <tr>
                    <td class="quantity">182</td>
                    <td class="color-cell" style="background-color: #73ffdf; color: black;">#73ffdf</td>
                    <td class="label">Marsh (182)</td>
                </tr>
                <tr>
                    <td class="quantity">183</td>
                    <td class="color-cell" style="background-color: #9ebbd7; color: black;">#9ebbd7</td>
                    <td class="label">Flooded flat (183)</td>
                </tr>
                <tr>
                    <td class="quantity">184</td>
                    <td class="color-cell" style="background-color: #828282;">#828282</td>
                    <td class="label">Saline (184)</td>
                </tr>
                <tr>
                    <td class="quantity">185</td>
                    <td class="color-cell" style="background-color: #f57ab6; color: black;">#f57ab6</td>
                    <td class="label">Mangrove (185)</td>
                </tr>
                <tr>
                    <td class="quantity">186</td>
                    <td class="color-cell" style="background-color: #66cdab; color: black;">#66cdab</td>
                    <td class="label">Salt marsh (186)</td>
                </tr>
                <tr>
                    <td class="quantity">187</td>
                    <td class="color-cell" style="background-color: #444f89;">#444f89</td>
                    <td class="label">Tidal flat (187)</td>
                </tr>
                <tr>
                    <td class="quantity">190</td>
                    <td class="color-cell" style="background-color: #c31400;">#c31400</td>
                    <td class="label">Impervious surfaces (190)</td>
                </tr>
                <tr>
                    <td class="quantity">200</td>
                    <td class="color-cell" style="background-color: #fff5d7; color: black;">#fff5d7</td>
                    <td class="label">Bare areas (200)</td>
                </tr>
                <tr>
                    <td class="quantity">201</td>
                    <td class="color-cell" style="background-color: #dcdcdc; color: black;">#dcdcdc</td>
                    <td class="label">Consolidated bare areas (201)</td>
                </tr>
                <tr>
                    <td class="quantity">202</td>
                    <td class="color-cell" style="background-color: #fff5d7; color: black;">#fff5d7</td>
                    <td class="label">Unconsolidated bare areas (202)</td>
                </tr>
                <tr>
                    <td class="quantity">210</td>
                    <td class="color-cell" style="background-color: #0046c8;">#0046c8</td>
                    <td class="label">Water body (210)</td>
                </tr>
                <tr>
                    <td class="quantity">220</td>
                    <td class="color-cell light-text" style="background-color: #ffffff;">#ffffff</td>
                    <td class="label">Permanent ice and snow (220)</td>
                </tr>
            </tbody>
        </table>
    </div>