"""
Module with all the code to create plots



"""


import sys, os, glob
import logging

import matplotlib
#matplotlib.use('Agg')
font = {'family' : 'sans-serif',
        'weight' : 'light',
        'size'   : 5}
matplotlib.rc('font', **font)
matplotlib.rcParams['timezone'] = 'Europe/Berlin'
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)

#from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
#from matplotlib.figure import Figure
import io

import pandas as pd
import database_logging

from optparse import OptionParser

logger = logging.getLogger(__name__)

hours = mdates.HourLocator(interval = 1)



def get_df_from_db(dbfilepath, nhours=5, mode=None):
    """
    if nhours, use past n hours

    if mode, ignores nhours and does something special
    """

    outdict = {}
    
    ldb = database_logging.LogDB(name=name)
    if mode is None:
        cmd = """SELECT * from {} WHERE 
            datetime > DATETIME('NOW', '-{} hour') 
            ORDER BY datetime""".format(name, nhours)
        
    elif mode == "yesterday":
        cmd = """SELECT * from {} WHERE 
            datetime >= DATETIME('NOW', 'start of day', "-1 day")
            and
            datetime < DATETIME('NOW', 'start of day')
            ORDER BY datetime""".format(name)

    df = pd.read_sql_query(cmd, ldb.con)
    ldb.close()
    df.datetime = pd.to_datetime(df.datetime, utc=True).map(lambda x: x.tz_convert('Europe/Berlin'))
    #df.datetime = df.datetime.tz_convert('Europe/Berlin')
    logger.info("Got dataframe {} of length {}".format(name, len(df)))
    outdict[name] = df
    return outdict

def get_df_from_csv(csvfilepath):
    """
    Low-level function to read a single csv file
    """
    if os.path.exists(csvfilepath):
        logger.info("Reading {} ...".format(csvfilepath))
        df = pd.read_csv(csvfilepath, delimiter="\t")
        df.datetime = pd.to_datetime(df.datetime, utc=True).map(lambda x: x.tz_convert('Europe/Berlin'))
        logger.info("Loaded dataframe of length {}".format(len(df)))
    else:
        df = None
        logger.warning("CSV file '{}' does not exist".format(csvfilepath))
    return df








def create_overview_fig(df, suptitle=None):

    logger.info("Creating overview figure...")
    

    if suptitle is None:
        #suptitle = df.attrs['title']
        #print(len(df))
        if len(df) > 3:
            #suptitle = df["datetime"][int(round(len(df)/2))].strftime("%a %d %b %Y")
            suptitle = df["datetime"][0].strftime("%a %d %b %Y")
        else:
            suptitle = "short df"

    #fig = plt.figure(figsize=(30,25), dpi=100) # (w, h)
    #fig = plt.figure(figsize=(15,12), dpi=200) # (w, h) # For subplots(4, 1)
    #fig = plt.figure(figsize=(10,10), dpi=200) # (w, h) # For subplots(4, 1)
    fig = plt.figure(figsize=(12,7)) # (w, h) # For subplots(4, 1)

    
    axes = fig.subplots(3, 1)
    plot_power(axes[0], df)
    plot_heat_temps(axes[1], df)
    plot_explore(axes[2], df)

    fig.suptitle(suptitle, horizontalalignment="right", verticalalignment="top", x=0.93, y=0.98, fontsize=10)

    fig.tight_layout()
    return fig

def closefig():
    # A bit ugly, but without this there is a memory leak.
    plt.close()

def getpng(fig):
    """
    If I rembember well this was useful to display an image (on the epaper screen) without writing to disk
    """

    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    return output

def write_daily_overview_fig(inputfilepath, outputfilepath):
    """
    
    use filepath = None to save to default location, in workdir
    """

    df = get_df_from_csv(inputfilepath)
    

    fig = create_overview_fig(df)
    fig.savefig(outputfilepath)
    logger.info("Wrote overview fig to {}".format(outputfilepath))

    closefig()


def set_time_axis(ax):
    ax.xaxis.set_minor_locator(AutoMinorLocator(4))
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))


def plot_power(ax, df):
    """Energy production and consumption"""

    ax.plot(df["datetime"], df["SMATripower_pgenerate"], label="Erzeugung", lw=1, color="green")
    
    ax.plot(df["datetime"], df["SMATripower_psupply"], label="Einspeisung", color="orange", lw=1, ls="--")
    #ax.plot(df["datetime"], df["SMAHomeManager_psupply"], label="SMAHomeManager_psupply")
     
    ax.plot(df["datetime"], df["SMATripower_ppurchase"], label="Kauf", lw=0.5, color="red")
    ax.fill_between(df["datetime"], df["SMATripower_ppurchase"], 0, color="red", alpha=0.2)
    #ax.plot(df["datetime"], df["SMAHomeManager_ppurchase"], label="SMAHomeManager_ppurchase")

    ax.plot(df["datetime"], df["VitocalOpen3E_CurrentElectricalPowerConsumptionSystem"], label="Wärmepumpe", lw=1, color="blue")
    

    # Write energy on fig
    epurchase = df["SMATripower_epurchase"][len(df)-1] - df["SMATripower_epurchase"][0]
    esupply = df["SMATripower_esupply"][len(df)-1] - df["SMATripower_esupply"][0]
    
    textstr = '\n'.join((
        f'Kauf: {epurchase:.2f} kWh',
        f'Einspeisung: {esupply:.2f} kWh'
        ))
    ax.text(0.85, 0.95, textstr, transform=ax.transAxes, fontsize=8,
        verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
    
    set_time_axis(ax)

    #ax.yaxis.set_major_locator(MultipleLocator(5))
    #ax.yaxis.set_minor_locator(MultipleLocator(1))
    ax.legend(loc="upper left")
    ax.grid(which="major", axis="x")
    ax.grid(which="both", axis="y", lw=0.5)
    ax.set_ylim(80, 8000)
    ax.set_ylabel('Leistung in W')
    ax.set_yscale("log")


def plot_heat_temps(ax, df):
    """Heating temperatures"""

    ax.plot(df["datetime"], df["VitocalOpen3E_DomesticHotWaterSensor_Actual"], label="WW", lw=1, color="red")
    ax.plot(df["datetime"], df["VitocalOpen3E_ReturnTemperatureSensor_Actual"], label="Return", lw=1, color="blue")
    ax.plot(df["datetime"], df["VitocalOpen3E_FlowTemperatureSensor_Actual"], label="Flow", lw=1, color="orange")
    ax.plot(df["datetime"], df["VitocalOpen3E_AllengraSensor_Temperature"], label="Allengra Temp", lw=1, color="purple")
    
    # SG-Ready
    ax.fill_between(df["datetime"], 0, 1, where = df["VitocalOpen3E_SmartGridReadyConsolidator_OperatingStatus"]  > 2,
                color='grey', alpha=0.2, transform=ax.get_xaxis_transform())


    set_time_axis(ax)

    #ax.yaxis.set_major_locator(MultipleLocator(5))
    #ax.yaxis.set_minor_locator(MultipleLocator(1))
    ax.legend(loc="upper left")
    ax.grid(which="major")
   


def plot_explore(ax, df):

    
    ax.plot(df["datetime"], df["VitocalOpen3E_AllengraSensor_Actual"], label="Allengra", lw=1, color="red")
    ax.plot(df["datetime"], df["VitocalOpen3E_SmartGridReadyConsolidator_OperatingStatus"], label="SG", lw=1, color="green")
     
    set_time_axis(ax)

    ax.legend(loc="upper left")
    ax.grid(which="major")
   

def plot_out(ax, dfs):
    """Outdoor conditions"""

    for name in ["S2"]:
        if len(dfs[name]) == 0: return
    
    ax.plot(dfs["S2"].datetime, dfs["S2"].temp, label="Temperatur °C", color="forestgreen")
   

    ax.set_ylim(-10, 35)
    ax.axhline(y=0, xmin=0, xmax=1, lw=2, color="black", ls="--")
    ax.set_ylabel("Temperatur °C", color="forestgreen")
    ax.xaxis.set_minor_locator(hours)
    ax.yaxis.set_major_locator(MultipleLocator(5))
    ax.yaxis.set_minor_locator(MultipleLocator(1))
    #ax.legend(loc="upper left")
    ax.text(0.5, 0.96, 'Außen', verticalalignment='top', horizontalalignment='center', transform=ax.transAxes, fontsize=15)
    ax.grid(which="both")
   

    twinax = ax.twinx()
    twinax.plot(dfs["uhr"].datetime, dfs["uhr"].pres, label="Uhr Luftdruck hPa", color="peru")
    twinax.set_ylabel('Luftdruck in hPa', color="peru")
    twinax.set_ylim(980, 1020)

    twinax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    


def plot_heiz(ax, dfs):
    """Heizung stuff"""
    df = dfs["heiz"]
    if len(df) == 0: return

    ax.plot(df.datetime, df.temp, label="Heizung Vorlauf °C", color="darkorange")
    ax.yaxis.set_minor_locator(MultipleLocator(5))
    ax.axhline(y=50, xmin=0, xmax=1, lw=2, ls="--", color="orange")
    ax.set_ylim(15, 70)
    ax.set_ylabel('Vorlauftemperatur in °C', color="darkorange")
    #ax.set_xlabel("UTC") # No, should now be with timezone...

    ax.text(0.5, 0.96, 'Heizung', verticalalignment='top', horizontalalignment='center', transform=ax.transAxes, fontsize=15)
    ax.xaxis.set_minor_locator(hours)
    #ax.legend(loc="upper left")
    ax.grid(which="both")

    twinax = ax.twinx()
    twinax.plot(df.datetime, df.micmag, label="Heizung Schall RMS", color="darkgrey", lw=0.5)
    twinax.set_ylabel('Schall RMS', color="darkgrey")
    twinax.set_ylim(40, 80)

    

    twinax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))



def plot_in(ax, dfs):
    """Indoor temperatur"""
    for name in ["wecker", "uhr"]:
        if len(dfs[name]) == 0: return
   
    
    ax.plot(dfs["uhr"].datetime, dfs["uhr"].temp, label="Wohnzimmer", color="deepskyblue")
    ax.plot(dfs["wecker"].datetime, dfs["wecker"].temp, label="Schlafzimmer", color="black", ls="None", marker='.')
    ax.set_ylim(17, 22)
    ax.yaxis.set_major_locator(MultipleLocator(1))
    #ax.yaxis.set_minor_locator(MultipleLocator(1))
    ax.set_ylabel('Temperatur in °C')
    ax.xaxis.set_minor_locator(hours)
    ax.legend(loc="upper left")
    ax.text(0.5, 0.96, 'Innen', verticalalignment='top', horizontalalignment='center', transform=ax.transAxes, fontsize=15)
    ax.grid(which="both")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))


def plot_humq(ax, dfs):
    """air quality"""

    for name in ["S2", "wecker", "uhr"]:
        if len(dfs[name]) == 0: return
   
    
    
    ax.plot(dfs["uhr"].datetime, dfs["uhr"].hum, label="Wohnzimmer", color="deepskyblue")
    ax.plot(dfs["wecker"].datetime, dfs["wecker"].hum, label="Schlafzimmer", color="black", ls="None", marker='.')
    ax.plot(dfs["S2"].datetime, dfs["S2"].hum, label="Außen", color="forestgreen")
    ax.set_ylabel('Relative Luftfeuchtigkeit in %')
    ax.axhline(y=75, xmin=0, xmax=1, lw=2, ls="--", color="black")
    ax.set_ylim(40, 100)
    ax.yaxis.set_major_locator(MultipleLocator(5))
    ax.yaxis.set_minor_locator(MultipleLocator(1))

    ax.xaxis.set_minor_locator(hours)
    ax.legend(loc="upper left")
    ax.text(0.5, 0.96, 'Luftqualität', verticalalignment='top', horizontalalignment='center', transform=ax.transAxes, fontsize=15)
    ax.grid(which="major")
    

    twinax = ax.twinx()
    twinax.plot(dfs["wecker"].datetime, dfs["wecker"].gas/100.0, label="Schlafzimmer Q Ohm", color="hotpink", ls="None", marker='.')
    twinax.set_ylabel('Schlafzimmer Q in Ohm', color="hotpink")
    twinax.set_ylim(50, 200)
    
    twinax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))



def write_daily_strom_fig(datestr=None, filepath=None, workdir="/home/pi/Databases/strom_figs"):
    """
    use datestr = None to read yesterday from sqlite dbs
    use datestr = "2012-12-05" to read from csv files

    use filepath = None to save to default location, in workdir
    """

    if datestr is None:
        dfs = getdfs(["strom"], mode="yesterday")
    else:
        dfs = getdfs_csv(["strom"], datestr, workdir="/home/pi/Databases")
    
    # Check if we have all data
    if dfs["strom"] is None:
        logger.warning("Data does not exist, skipping figure creation")
        return
    
    suptitle = dfs["strom"].datetime[int(len(dfs["strom"].datetime)/2)].strftime('%Y-%m-%d') # datetime from central point
    
    if filepath is None: # Then we create a default one
        os.makedirs(workdir, exist_ok=True)
        filepath = suptitle + ".png"
        filepath = os.path.join(workdir, filepath)


    fig = create_strom_fig(nhours=None, suptitle=suptitle, dfs=dfs)
    fig.savefig(filepath)
    logger.info("Wrote strom fig to {}".format(filepath))

    closefig()

def create_strom_fig(nhours=None, mode=None, dfs=None, suptitle=None):

    logger.info("Creating figure...")
    if dfs is None:
        dfs = getdfs(["strom"], nhours=nhours, mode=mode)
    
    #fig = plt.figure(figsize=(30,25), dpi=100) # (w, h)
    fig = plt.figure(figsize=(20,5), dpi=200) # (w, h)
    
    (ax) = fig.subplots(1, 1)
    plot_strom(ax, dfs)
    
    fig.suptitle(suptitle, horizontalalignment="right", verticalalignment="top", x=0.95, y=0.95, fontsize=20)
    fig.tight_layout()
    return fig

def plot_strom(ax, dfs, epaper=False):
    """stromverbrauch"""

    df = dfs["strom"]
    if len(df) == 0: return
    logger.info("Got {} original records".format(len(df)))

    # Kick rows closing an original interval that is very narrow
    # No longer needed, now that we limit frequency of logs at the sensor level
    #df["seconds_prev_origint"] = (df['datetime'] - df['datetime'].shift(1)).dt.total_seconds()
    #df.drop(df[df.seconds_prev_origint < 60].index, inplace=True)
    #logger.info("After purge, keeping {} records".format(len(df)))

    # Compute number of Rotations in ending interval (i.e., interval right before this datetime)
    df["diffcount_ending_int"] = df['count'] - df['count'].shift(1) # Is 1 if no records are missing, but may also be 2, 3,...
    
    # If negative or zero (due to reset of sensor), set it to 1 after showing it as vertical line
    reset_mask = df["diffcount_ending_int"] <= 0
    for i in range(1, len(reset_mask)): # we skip the first
        if reset_mask.iloc[i] == True:
            ax.axvline(df.datetime.iloc[i], color="red", linewidth=3)
    df["diffcount_ending_int"] = df["diffcount_ending_int"].clip(lower=1, upper=None)
    
    # Compute duration of ending interval:
    df["seconds_ending_int"] = (df['datetime'] - df['datetime'].shift(1)).dt.total_seconds()
    
    # Compute energy of ending interval, in Wh
    # 75 U / kWh -> 1 count is (1000/75) Wh
    df["energy_ending_int"] = df["diffcount_ending_int"] * (1000./75)

    total_energy = df["energy_ending_int"].sum()
    if not epaper:
        ax.text(0, 1, "\n Gesamt: {:.1f} Wh".format(total_energy),
            horizontalalignment='left',
            verticalalignment='top',
            fontsize=15,
            transform=ax.transAxes)

    # Average power in W of ending interval:
    df["power_ending_int"] = df["diffcount_ending_int"] * (1000./75) / ( df["seconds_ending_int"] / 3600 )
    
    # And the actual plot

    # Works only with recent Matplotlib:
    #ax.stairs(df.power_ending_int[1:], df.datetime, label="Leistung", color="black")
    # Older Matplotlibs:
    lw = 1.0
    color = "black"
    if epaper:
        lw=1.0
        color="red"
    ax.step(df.datetime, df.power_ending_int, where="pre", label="Leistung", color=color, lw=lw)
    #ax.fill_between(df.datetime, df.power_ending_int, y2=50, where="pre", interpolate=False, step=None, facecolor=color, lw=0)
    
    ax.set_ylim(50, 5000)
    if not epaper:
        ax.set_ylabel('Leistung in W')
    ax.set_yscale("log")
    #ax.plot(df.datetime, df.dip_duration)
    #ax.set_ylim(0, 5)
    
    #print(df.datetime[1:])
    #print(df.power_prev_int)
    ax.xaxis.set_minor_locator(hours)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    if epaper:
        #ax.set_xticklabels(ax.get_xticks(), rotation=45)
        for tick in ax.get_xticklabels():
            tick.set_rotation(60)


    # Now the processing of peaks
    detect_power = 500
    #ax.axhline(detect_power, color="lightgray", dashes=(2,5))
    baseline_power = 250

    if not epaper:
        ax.axhline(baseline_power, color="lightgray", dashes=(5,5))
    
    peaks = []
    inpeak = False
    nonpeak_energy = 0

    # Now the ugly loop

    for i in range(1, len(df["power_ending_int"])): # we skip the first
        power_ending_int = df["power_ending_int"].iloc[i]
        seconds = df["seconds_ending_int"].iloc[i]
        if power_ending_int > detect_power:
            if not inpeak: # We start a peak
                inpeak = True # Note that the "if" below will run to take this energy in account for the peak
                peak = {"energy":0, "starti":i-1}
            if inpeak: # We accumulate energy
                extra_power = power_ending_int - baseline_power
                peak["energy"] += extra_power * ( seconds / 3600 )
                nonpeak_energy += baseline_power * ( seconds / 3600 )

        else:
            if inpeak:
                # We finalize the peak
                inpeak = False # Note that the "if" below will run to count this interval behind the last peak as non_peak energy
                peak["endi"] = i-1
                peaks.append(peak)
            if not inpeak: # Nothing to do
                nonpeak_energy += power_ending_int * ( seconds / 3600 )

    
    labelypos = [1500, 700]
    textcolor="red"
    if epaper:
        textcolor="black"
    for (i, peak) in enumerate(peaks):
        if not epaper:
            ax.axvline(df.datetime.iloc[peak["starti"]], color="lightgray", dashes=(3, 3))
            ax.axvline(df.datetime.iloc[peak["endi"]], color="lightgray", dashes=(3, 3))
        centeri = int((peak["starti"] + peak["endi"]) / 2)
        ax.text(df.datetime.iloc[centeri], labelypos[i % 2], "{:.0f}".format(peak["energy"]),
            horizontalalignment='center', fontsize=15, color=textcolor, rotation=90)
    
    ax.text(df.datetime.iloc[int(len(df)/2)], baseline_power/2, "{:.0f}".format(nonpeak_energy),
            horizontalalignment='center', fontsize=15, color=textcolor)
    

    
    """
    df["extra_power_ending_int"] = (df["power_ending_int"] - baseline_power).clip(lower=0, upper=None)
    df["extra_energy_ending_int"] = df["extra_power_ending_int"] * ( df["seconds_ending_int"] / 3600 ) / 1000.0 # in kWh
    #df["extra_energy_cumsum_ending_int"] = df["extra_energy_ending_int"].cumsum()
    #print(df)
    
    extra_energy_ending_int = df["extra_energy_ending_int"]
    groupsums = []
    groupis = []
    groupsum = 0
    for i in range(len(extra_energy_ending_int)):
        e = extra_energy_ending_int[i]
        if e > 0:
            groupsum += e
        else: #e == 0
            if groupsum > 0: # then we end a group!
                groupis.append(i-1)
                groupsums.append(groupsum)
                groupsum = 0
    #print(groupis)
    #print(groupsums)
    for i in range(len(groupis)):
        ax.axvline(df.datetime[groupis[i]], color="orange")
        ax.text(df.datetime[groupis[i]], baseline_power, "{:.3f}".format(groupsums[i]))
    """


# def plot_pres(ax, dfs):
#     #hours = mdates.HourLocator(interval = 1)
#     ax.plot(dfs["uhr"].datetime, dfs["uhr"].pres, label="Uhr Luftdruck hPa")
#     ax.xaxis.set_minor_locator(hours)
#     ax.legend(loc="upper left")
#     ax.grid(which="both")



if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    parser = OptionParser()

    parser.add_option("-t", action="append", type="string", dest="todolist", default=None)
    parser.add_option("-f", "--file", action="store", type="string", dest="filename", default=None)

    (options, args) = parser.parse_args()

    logging.info(options)


    write_daily_overview_fig("/home/mtewes/data/pvpi/2025/2025-01-03.csv", "/home/mtewes/test.pdf")


"""


    if "test" in options.todolist:
        
        #write_daily_overview_fig(filepath="test.png") # uses yesterday, from sqlite dbs

        # Use this to recreate indivudal figures:
        write_daily_overview_fig(datestr="2025-01-28")


        #dfs = getdfs_csv(sensornames, datestr="2022-12-05")
        #write_daily_overview_fig(datestr="2022-12-05") # reads csv
        #
        #fig = createfig(nhours=12)
        #fig.savefig("test_plotdb.png")
        #logger.info("Done!")
        #

    if "daily_overview" in options.todolist:
        write_daily_overview_fig()
    if "daily_strom" in options.todolist:
        write_daily_strom_fig()
    

    if "daily_overview_all" in options.todolist:
        # Redo everything from a particular year
        year = "2022"

        # Find dates by searching files...
        workdir="/home/pi/Databases"
        name = "heiz"
        pattern = os.path.join(workdir, name, year, "*.csv")
        print(pattern)
        files = sorted(glob.glob(pattern))
        datestrs = [os.path.split(file)[1][:-4] for file in files]
        print(datestrs)
        for datestr in datestrs:
            write_daily_overview_fig(datestr=datestr)
    
    if "strom" in options.todolist:
        if options.filename:
            
            df = getdf_csv(options.filename)
            dfs = {"strom":df}
            fig = create_strom_fig(dfs=dfs)
            fig.savefig("strom.png")

        else:
            
            write_daily_strom_fig()

            #dfs = getdfs(["strom"], nhours=5, mode=None)
            #print(dfs["strom"])
            #fig = create_strom_fig(dfs=dfs)
            #fig.savefig("strom.png")

"""