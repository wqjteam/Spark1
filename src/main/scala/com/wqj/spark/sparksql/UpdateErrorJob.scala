package com.wqj.spark.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object UpdateErrorJob {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UpdateErrorJob").setMaster("local[*]")
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "true")
    sparkConf.set("spark.debug.maxToStringFields", "300")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer", "128")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //    if (args.length == 0) {
    //      println("---------------------------------请传参数类似于:tboxdata/data/daily/channel/20180816/IP34----------------------------------------")
    //      System.exit(0)
    //    }
    //    for (x <- args(1).split(",")) {

    val x = "D:\\java_space\\tbox_data_process\\tboxdata\\data\\streaming\\channel\\20180823\\15\\24_1535009215853"

    sqlContext.read.schema(ChannelSchema.schema).parquet(x).createOrReplaceTempView("tmp")
    //注册两个临时函数
    //转0.001
    sqlContext.udf.register("conversionAccuracy", Conversion.ConversionAccuracy)
    //转0.5
    sqlContext.udf.register("conversionAccuracyPro", Conversion.ConversionAccuracyPro)
    //编写sql
    sqlContext.sql("select tboxid,vin,data_date,gnss_time,lon,lat,alt,direction,gnsssats,speed,vehrpm,power_mode," +
      "gear_pos,ax,ay,az,a_pos,b_pos,steer_angle,fuel_consumed,vehdoorfrontpas,vehdoorfrontdrv,vehdoorrearleft," +
      "vehdoorrearright,vehbonnet,vehboot,vehwindowfrontleft,vehwindowrearleft,vehwindowfrontright,vehwindowrearright," +
      "vehsunroof,vehcruiseactive,vehcruiseenabled,vehcruisetargetspeed,vehoutsidetemp,vehinsidetemp,vehac,vehacauto," +
      "vehaccircdirection,vehaccirctype,vehacfanspeed,vehacdrvtargettemp,vehacpasstargettemp,vehseatbeltdrv," +
      "vehseatbeltpas,vehindlightleft,vehindlightright,vehsidelight,vehdiplight,vehmainlight,vehfoglightfront," +
      "vehfoglightrear,vehwiperswitchfront,vehraindetected,vehnightdetected,vehfuellev,vehbatt,vehcoolanttemp,mile," +
      "vehdistrollcount,vehhorn,vehoilpressurewarning,vehmilwarning,vehdrivebywirewarning,cellmcc,cellmnc,cellsignalstrength," +
      "cellrat,celllac,cellcellid,cellchanid,original_longitude_change,original_latitude_change,grid,diff_date_pre,diff_date_lat," +
      "diff_gps_pre,diff_gps_lat,diff_mile_pre,diff_mile_lat,diff_v_pre,diff_v_lat,pre_direction,lat_direction,hdop," +
      "gpsstatus,partnumber,version,engineering_model,sub_engineering_model,c_30_dtcinfomationtcm,c_30_dtcinfomationecm," +
      "c_30_dtcinfomationbcm,vehworkmodel_g,veheptrdy,vehbmsbscsta,vehspdavgdrvnv,vehodov,vehbmspackvolv,vehbmspackvol," +
      "vehbmspackcrntv,vehbmspackcrnt,vehbmspacksocv,vehbmspacksoc,vehhvdcdcsta,vehepttrinptshafttoq,vehepttrinptshafttoqv," +
      "veheptbrkpdldscrtinptsts,vehbrksysbrklghtsreqd,vehepbsysbrklghtsreqd,vehtrshftlvrposv,vehbmsptisltnrstc,vehbmsptisltnrstcv," +
      "veheptaccelactuposv,veheptbrkpdldscrtinptstsv,vehtminvtrcrntv,vehtminvtrcrnt,vehtmsta,vehtminvtrtem,vehtmspdv,vehtmspd," +
      "vehtmactutoqv,vehtmactutoq,vehtmsttrtem,vehhvdcdchvsidevolv,vehhvdcdchvsidevol,vehisginvtrcrntv,vehisginvtrcrnt," +
      "vehisgsta,vehisginvtrtem,vehisgspdv,vehisgspd,vehisgactutoqv,vehisgactutoq,vehisgsttrtem,vehenspdsts,vehavgfuelcsump_g," +
      "vehbmscellmaxvolindx,vehbmscellmaxvol,vehbmscellmaxvolv,vehbmscellminvolindx,vehbmscellminvol,vehbmscellminvolv," +
      "vehbmscellmaxtemindx,vehbmscellmaxtem,vehbmscellmaxtemv,vehbmscellmintemindx,vehbmscellmintem,vehbmscellmintemv," +
      "vehhvdcdctem,vehbrkfludlvllow,vehbrksysredbrktlltreq,vehabsf,vehvsests,vehibstrwrnngio,vehbmshvilclsd,vehelecvehsysmd," +
      "veheptenfuelpumponreq,vehchargerhvvolt,vehchargerhvcurrent,vehbmscellvoltindex,conversionAccuracy(vehbmscellvolt) as vehbmscellvolt," +
      "vehbmscelltemindx,conversionAccuracyPro(vehbmscelltem) as vehbmscelltem,bmstotaldtc,dtcinfomationbms,ecmtotaldtc,dtcinfomationecm," +
      "tmtotaldtc,dtcinfomationtc,isgtotaldtc,dtcinfomationisc,vehtminvtrvolv,vehtminvtrvol,vehbmschrgstsio," +
      "vehbmsbalancingstatus,vehbmsavlblenrg,keep_network_bms,wake_network_bms,vehtrshftlvrpos,vehelecmotemgcshutdwn," +
      "vehepthvemgcpwroffreq,vehspdavgdrvn,veheptaccelactupos,veheptfuelcutreq,vehtrestdgear,veheptmainrelaydrvreq," +
      "vehepthvdcdcmdreq,veheptisgmdreq,vehepttmmdreq,vehtmsideclsts,vehisgsideclsts,vehepttmmaxtoqlmt,vehepttmmintoqlmt," +
      "vehepteduoilprs,vehtmrtrtem,vehtmhvinhd,vehtmfltio,vehhvdcdcoverhtd,vehisgrtrtem,vehisghvinhd,vehisgfltio,vehtmmaxavlbltoq," +
      "vehtmmaxavlbltoqv,vehtmminavlbltoq,vehtmminavlbltoqv,vehtminvtrclnttem,vehhvdcdclvsidevol,vehhvdcdclvsidevolv," +
      "vehhvdcdclvsidecrnt,vehhvdcdclvsidecrntv,vehtmclntpumpsts,vehisgmaxavlbltoq,vehisgmaxavlbltoqv,vehisgminavlbltoq," +
      "vehisgminavlbltoqv,vehisginvtrclnttem,vehmsfrdiagctr,vehhybenfltsts,vehenclnttem,vehenclnttemv,vehenactustdystatoq," +
      "vehenactustdystatoqv,vehenoilprslowio,vehennonemsnrltdmalfa,vehenoilchngio,vehenspd,vehenemsnrltdmalfa,vehfuelcsump," +
      "vehenoilprs,gear_transform_rule,powerstatus,bmschargestatus,bmsbatteryvoltage1,bmsbatterycurrent1,vehbmspacksoc_dt," +
      "ldc_status1,hvisolation,brakepedalposition,motorstatus,mctemp,n_mot,tq_mot,motortemp,v_ept,i_mot,bmscellvoltagemaxcellnumber," +
      "bmscellvoltagemax,bmscellvoltagemincellnumber,bmscellvoltagemin,bmstempmaxprobenumber,bmsprobetempmax,bmstempminprobenumber," +
      "bmsprobetempmin,bmsbatteryvoltage2,bmsbatterycurrent2,bmscellvoltagem,bmstempprobequantity,bmsprobetempm,bmswarninginformation," +
      "socfastchangewarning,bldc_err_ot,absfault,ldc_status2,bept_err_ot_igbt,hvilwarning,bept_err_ot_mot,bmsvoltagehighwarning," +
      "bmstotaldtc_dt,dtcinfomationbms_dt,tmtotaldtc_dt,dtcinfomationtc_dt,pre_model,lat_model,diff_batt_pre,diff_batt_lat," +
      "pre_vehisgspd from tmp").coalesce(1)
      .show()
    //      .write.parquet(x + "/tmp")


    //将单一文件替代旧文件

    //    HdfsUtils.deleteFileExceptFolder(x)

    //    }

  }
}
