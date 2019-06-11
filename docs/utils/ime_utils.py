import sys
import os
import re
from datetime import date, datetime, timedelta
import logging
import json
import copy
from dateutil import relativedelta

ROI_PREDICT_DAY = 90
ROI_LT_DAYS = [30, 45, 60, 75, ROI_PREDICT_DAY]
ROI_TAG_LIST = ['', '_ecpmdown_40']
ROI_KEYS = ["country", "source_type", "media_source", "campaign_id", 'partner', 'channel_code', 'recommend_channel', 'app_name']
crash_actions = ['app_crash', 'KEYBOARD_WINDOW_SHOWN', 'ON_ACTIVITY_STOP']
ACTIVE_PREFIX = ['', 'bg_', 'rdau_']
#def etl_type_name(app_name, channel_code, recommend_channel):
#     if not recommend_channel:
#         recommend_channel = channel_code
#     if app_name and recommend_channel and channel_code\
#             and ('cootek.smartinput.international.android' in app_name or 'cootek.smartinput.mainland.android' in app_name)\
#             and (recommend_channel.startswith('OEM') or channel_code.startswith('OEM')):
#         return 'com.cootek.smartinputv5'
#     else:
#         return app_name

TYPE_NAME_CHN1 = {'OEM 048 AA6 102', 'OEM 002 A15 001', 'OEM 16V ABX 001', 'OEM 048 03A 035', 'OEM 087 A15 001',
                  'OEM 11B A15 001', 'OEM 14P A9Y 001', 'OEM 154 ABR 001', 'OEM 049 A15 207', 'OEM 049 A17 020',
                  'OEM 007 A03 001', 'OEM 002 A15 005', 'OEM 016 A22 001', 'OEM 034 A22 001', 'OEM 015 A75 001',
                  'OEM 049 A17 025', 'OEM 00B A17 001', 'OEM 150 A9Y 001', 'OEM 184 A9Y 001', 'OEM 09D A22 001',
                  'OEM 03D A15 001', 'OEM 014 A23 001', 'OEM 002 A9U 001', 'OEM 054 A75 001', 'OEM 015 A22 003',
                  'OEM 048 AA6 107', 'OEM 048 A75 001', 'OEM 11D A15 001', 'OEM 001 A07 001', 'OEM 049 A45 001',
                  'OEM 11B A15 002', 'OEM 049 A15 201', 'OEM 016 A15 001', 'OEM 060 A17 001', 'OEM 048 A9E 001',
                  'OEM 01E A15 001', 'OEM 068 A15 001', 'OEM 049 A17 021', 'OEM 015 A15 001', '0EM 015 A22 003',
                  'OEM 00B A22 001', 'OEM 049 A17 024', 'OEM 14E A22 001', 'OEM 048 03A 008', 'OEM 048 AA6 103',
                  'OEM 001 A25 002', 'OEM 124 A15 001', 'OEM 049 A17 008', 'OEM 049 A17 009', 'OEM 11A A23 000',
                  'OEM 127 A15 001', 'OEM 01C A75 001', 'OEM 05D A15 001', 'OEM 13D A17 001', 'OEM 14G A22 001',
                  'OEM 166 A75 001', 'OEM 049 A15 206', 'OEM 049 A17 022', 'OEM 03D A17 002', 'OEM 049 A15 101',
                  'OEM 048 A94 001', 'OEM 034 A15 000', 'OEM 054 A73 001', 'OEM 058 A9H 001', 'OEM 017 A03 001',
                  'OEM 14F A22 001', 'OEM 048 AA6 104', 'OEM 03D A15 000', 'OEM 197 ABR 001', 'OEM 049 A15 208',
                  'OEM 008 A15 001', 'OEM 048 03A 030', 'OEM 006 A36 001', 'OEM 07E A25 001', 'OEM 124 A15 003',
                  'OEM 09D A29 001', 'OEM 015 A22 001', 'OEM 14M A22 001', 'OEM 002 A9Y 001', 'OEM 049 A17 006',
                  'OEM 008 A17 001', 'OEM 143 A15 001', 'OEM 001 A25 003', 'OEM 059 A14 001', 'OEM 041 A22 003',
                  'OEM 049 A15 203', 'OEM 012 A9H 001', 'OEM 07E A15 001', 'OEM 115 A9B 001', 'OEM 048 A15 002',
                  'OEM 14C A22 000', 'OEM 00B A64 003', 'OEM 00B A22 002', 'OEM 001 A03 003', 'OEM 049 A15 205',
                  'OEM 048 ABB 002', 'OEM 007 A22 003', 'OEM 09D A29 003', 'OEM 048 A9B 001', 'OEM 03D A25 001',
                  'OEM 007 A22 001', 'OEM 14H A22 001', 'OEM 09D A29 002', 'OEM 048 ABB 001', 'OEM 002 A75 001',
                  'OEM 00B AA3 001', 'OEM 049 A17 004', 'OEM 002 A9H 001', 'OEM 00B A22 003', 'OEM 03D A03 001',
                  'OEM 048 AA7 001', 'OEM 184 ABB 001', 'OEM 048 ABH 001', 'OEM 048 A68 002', 'OEM 015 A75 002',
                  'OEM 11A A15 003', 'OEM 001 A03 002', 'OEM 049 A15 103', 'OEM 048 AA6 101', 'OEM 01E A22 001',
                  'OEM 058 A15 001', 'OEM 07D A03 002', 'OEM 048 03A 033', 'OEM 048 03A 027', 'OEM 049 A17 023',
                  'OEM 049 A17 026', 'OEM 03D A22 001', 'OEM 049 A9K 001', 'OEM 00B AA9 001', 'OEM 049 A17 019',
                  'OEM 008 A22 001', 'OEM 162 A75 001', 'OEM 048 03A 039', 'OEM 001 A25 001', 'OEM 11D A23 001',
                  'OEM 049 A15 202', 'OEM 048 A9B 101', 'OEM 048 03A 025', 'OEM 048 A84 001', 'OEM 041 A22 004',
                  'OEM 01C A15 005', 'OEM 001 A03 001', 'OEM 014 A24 001', 'OEM 115 A15 004', 'OEM 048 A17 001',
                  'OEM 03D A22 010', 'OEM 058 A22 001', 'OEM 15X A75 001', 'OEM 048 AA6 108', 'OEM 048 03A 020',
                  'OEM 049 A15 204', 'OEM 049 A17 007', 'OEM 049 A15 102', 'OEM 055 ABB 001', 'OEM 142 A15 001',
                  'OEM 015 A84 001', 'OEM 049 A17 005', 'OEM 115 A15 003', 'OEM 049 A17 017', 'OEM 049 A17 018',
                  'OEM 048 ABR 001', 'OEM 049 A17 002', 'OEM 041 A15 001', 'OEM 049 A17 003', 'OEM 01E A03 002',
                  'OEM 00B A94 001', 'OEM 049 A17 027', 'OEM 048 03A 029', 'OEM 009 A15 001', 'OEM 068 A22 001',
                  'OEM 049 A17 001', 'OEM 049 A17 016', 'OEM 01C A22 001', 'OEM 048 A15 001'}

TYPE_NAME_CHN3 = {'OEM 01C A03 001', 'OEM 001 A03 000', 'OEM 01C A03 002', 'OEM CTK A03 001', 'OEM 017 A36 001',
                  'OEM 058 A03 001', 'OEM 097 A03 000', 'OEM 017 A03 000', 'OEM 007 A03 000', 'OEM 002 A03 000'}

mopub_re = re.compile(r"^[0-9a-z]+$")

#this func should be a replacement to etl_type_name
#def etl_app_name_oem2online(app_name):
def etl_type_name(app_name, channel_code, recommend_channel):
    recommend_channel = recommend_channel if recommend_channel else channel_code
    if app_name == 'cootek.smartinput.international.android.oem.free':
        if recommend_channel in TYPE_NAME_CHN1:
            return 'cootek.smartinput.international.android.public'
        elif recommend_channel in TYPE_NAME_CHN3:
            return 'cootek.smartinput.international.android.publicthird.1505'
        else:
            return 'cootek.smartinput.international.android.public'
    elif app_name == 'cootek.smartinput.international.android.publicoem.1505':
        return 'cootek.smartinput.international.android.public.1505'
    elif app_name == 'cootek.smartinput.international.android.oem':
        return 'cootek.smartinput.international.android.public.oem.1507'
    else:
        return app_name
    

app2package = {
    'cootek.smartinput.international.android.public': 'com.cootek.smartinputv5',
    'cootek.smartinput.mainland.android.oem.touchpalx': 'com.cootek.smartinputv5',
    'cootek.smartinput.mainland.android.public': 'com.cootek.smartinputv5',
    'cootek.smartinput.international.android.public.oem.1507': 'com.cootek.smartinputv5.oem',
    'cootek.smartinput.international.android.publicthird.1505': 'com.cootek.smartinputv5.freeoem',
    'cootek.smartinput.international.android.publicoem.1505': 'com.emoji.keyboard.touchpal',
    'cootek.smartinput.international.android.public.1505': 'com.emoji.keyboard.touchpal',
    'cootek.smartinput.international.android.publicoem.1603': 'com.emoji.keyboard.touchpal.oem',
    'cootek.smartinput.international.android.public.com.emoji.keyboard.touchpal.vivo': 'com.emoji.keyboard.touchpal.vivo',
    'cootek.smartinput.international.android.public.com.emoji.keyboard.touchpal.lenovo': 'com.emoji.keyboard.touchpal.lenovo',
    'cootek.smartinput.international.android.public.com.emoji.keyboard.touchpal.lava': 'com.emoji.keyboard.touchpal.lava',
    'cootek.smartinput.international.android.public.emoji.keyboard.teclado': 'emoji.keyboard.teclado',
    'cootek.smartinput.international.android.public.abc.apple.emoji.theme.gif.keyboard': 'abc.apple.emoji.theme.gif.keyboard',
    'cootek.smartinput.international.android.public.emoji.keyboard.color.gif': 'emoji.keyboard.color.gif',
    'cootek.smartinput.international.android.publicoem.1604.zte.f907': 'com.emoji.keyboard.touchpal.zte.f907',
}

matrix_package_name_list = [
    'abs.workout.fitness.tabata.hiit.stomach',
    'bluelight.filter.battery.life.sleep.night',
    'bluelight.filter.sleep.warmlight.eyes.battery',
    'clash.crush.fun.emoji.game',
    'coin.dozer.pusher.claws.prizes.chips',
    'com.eyefilter.night',
    'com.flashlight.brightest.beacon.torch',
    'com.freewallpapers.backgrounds',
    'com.health.drinkwater.reminder',
    'com.incoming.call.screen.flash.show',
    'com.led.flashlight.torch.beacon',
    'com.phonedialer.contact',
    'com.podcast.radioalarm',
    'com.seven.fitness.workout',
    'com.touchist.locker.todolist',
    'com.touchist.locker.todolist.reminder',
    'drinkwater.reminder.h2o.hydration.intake.tracker',
    'ios.fit.workout.fitness.exercise.trainer.tracker',
    'music.video.player.videoplayer.online.free',
    'swift.free.phone.call.wifi.chat',
    'swift.free.phone.call.wifi.chat.ios',
    'swift.free.wifi.phone.call.chat',
    'touchist.todolist.reminder',
    'word.chef.cookies.connect.cooking.puzzle',
    'free.phone.call.abroad.overseas.calling',
    'man.fit.workout.routine.muscle.training',
    'com.online.free.music.video.player',
    'com.color.callscreen.incomingcall.theme',
    'word.scramble.free.connect.puzzle',
    'steps.counter.pedometer.pacer.fitness.run',
    'horoscope.zodiac.sign.astrology.free',
    'daily.horoscope.zodiac.sign.astrology.free',
    'period.tracker.menstrual.cycle.conception.calendar',
    'com.color.caller.screen.flash',
    'com.water.drink.reminder.tracker.health',
    'com.color.flashlight.hd.torch',
    'pixel.draw.coloring.number.unicorn.art',
    'com.my.music.player.free',
    'com.horoscope.daily.zodiac.free',
    'com.music.player.media.mp3',
    'com.deepsleep.soft.music.relax',
    'com.screen.nightfilter.eyecare',
    'com.alarmclock.digital.timer',
    'com.feka.onedraw.oneline.drawing.game',
    'com.free.daily.horoscope.zodiac.secret',
    'com.deepsleep.sleep.soft.music.sounds',
    'com.alarmclock.digital.timer',
    'com.screen.dimmer.nightfilter.nightmode',
    'cats.word.connect.chef.puzzle.game',
    'com.one.line.drawing.linking.puzzle',
    'step.tracker.stepcounter.walking',
    'com.qrcode.barcode.reader.scanner.free',
    'com.period.tracker.menstrual.cycle.cherry',
    'hifit.workout.butt.fitness.weightloss',
    'com.music.hot.trend.songs.free',
    'com.color.call.flash.colorphone',
    'com.water.drink.hydro.alarm.timer',
    'com.color.phone.flash.caller.screen',
    'dacall.overseas.free.call.wifi.india',
    'com.horoscope.palmistry.zodiac.daily',
    'com.selfie.camera.beauty.makeup',
    'com.predict.horoscope.daily.zodiac.sign',
    'com.internet.radio.fm.podcast',
    'dacall.overseas.free.call.wifi.bangladesh',
    'dacall.overseas.free.call.wifi.pakistan',
    'com.document.pdf.scanner.docscan',
    'com.fast.messages.social.messenger.free',
    'com.period.calendar.rosa.menstrual.tracker',
    'walking.pedometer.fit.stepcounter',
    'workout.booty.burnfat.loseweight.absworkout',
    'stepcounter.pedometer.steps.steptracker',
    'com.video.live.wall.paper.background.free',
    'workout.fitness.training.abs.loseweight',
]

# appsflyer skin product
ua_plugin_package_name_list = [
    'com.cootek.smartinputv5.skin.keyboard_theme_live_neon_heart_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_theclownfish2',
    'com.cootek.smartinputv5.skin.keyboard_theme_water',
    'com.cootek.smartinputv5.skin.keyboard_theme_sparkling_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_diamond_princess',
    'com.cootek.smartinputv5.skin.keyboard_theme__cute_pink_unicorn_keyboard_',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_butterfly_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_romantic_couple_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_water_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_butterfly_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_neon_heart_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_pink_unicorn_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_romantic_couple_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_theclownfish2_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_sparkling_heart_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_diamond_princess_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_rose_gold_silk_os_11_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_rose_gold_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_blue_water_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_galaxy_dream_catcher_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_lovely_cute_cat_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_corcovado_mountain',
    'com.cootek.smartinputv5.skin.keyboard_theme_green_light',
    'com.cootek.smartinputv5.skin.keyboard_theme_brazil_rio_christ_the_redeemerr',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_pet_cat',
    'com.cootek.smartinputv5.skin.keyboard_theme_dark_rose',
    'com.cootek.smartinputv5.skin.keyboard_theme_easter_day_bunny',
    'com.cootek.smartinputv5.skin.keyboard_theme_color_easter_eggs',
    'com.cootek.smartinputv5.skin.keyboard_theme_easter_fire_phoenix',
    'com.cootek.smartinputv5.skin.keyboard_theme_ios10_galaxy_wallpaper',
    'com.cootek.smartinputv5.skin.keyboard_theme_golden_white_house_trump',
    'com.cootek.smartinputv5.skin.keyboard_theme_green_life',
    'com.cootek.smartinputv5.skin.keyboard_theme_green_skull_gun',
    'com.cootek.smartinputv5.skin.keyboard_theme_hologram_neon',
    'com.cootek.smartinputv5.skin.keyboard_theme_magic_blue',
    'com.cootek.smartinputv5.skin.keyboard_theme_mothers_love',
    'com.cootek.smartinputv5.skin.keyboard_theme_purple_dream_galaxy',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_sakura',
    'com.cootek.smartinputv5.skin.theme_valentine_secret_love',
    'com.cootek.smartinputv5.skin.keyboard_theme_flying_skull_tattoo',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_pink_butterfly',
    'com.cootek.smartinputv5.skin.keyboard_theme_spring_easter_day',
    'com.cootek.smartinputv5.skin.keyboard_theme_stay_with_you',
    'com.cootek.smartinputv5.skin.keyboard_theme_wild_leopard',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_sexy_panther',
    'com.cootek.smartinputv5.skin.theme_sweet_candy',
    'com.cootek.smartinputv5.skin.keyboard_theme_wild_wolf',
    'com.cootek.smartinputv5.skin.theme_fun_halloweencat',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_red_neon_heart_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_sparkling_minny_bowknot_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_swimming_fish_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_koi_fish_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_butterfly_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_sparkling_galaxy_diamond',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_red_flaming_fire',
    'com.cootek.smartinputv5.skin.keyboard_theme_black_red_style',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_romantic_couple_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_3d_falling_gold_coins',
    'com.cootek.smartinputv5.skin.keyboard_theme_diwali_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_golden_gorgeous_rose',
    'com.cootek.smartinputv5.skin.keyboard_theme_buddha_lotus',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_music',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_cute_emoji_love',
    'com.cootek.smartinputv5.skin.keyboard_theme_thanksgiving_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_falling_water_droplets_new',
    'com.cootek.smartinputv5.skin.keyboard_theme_sakura_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_galaxy_3d_hologram',
    'com.cootek.smartinputv5.skin.keyboard_theme_white_cute_love_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_simple_black_pink',
    'com.cootek.smartinputv5.skin.keyboard_theme_spent_halloween_together',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_purple_halloween',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_light_butterfly',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_halloween_graveyard',
    'com.cootek.smartinputv5.skin.keyboard_theme_blue_bow_zipper',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_live_koi_fish',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_halloween_ghost_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_happy_halloween_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_black_wolf',
    'com.cootek.smartinputv5.skin.keyboard_theme_purple_glitter',
    'com.cootek.smartinputv5.skin.keyboard_theme_mid_autumn_festival',
    'com.cootek.smartinputv5.skin.keyboard_theme_red_rose_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_galaxy_unicorn_flower',
    'com.cootek.smartinputv5.skin.keyboard_theme_rose_gold_minny',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_diamonds_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_panda_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_christmas_jingle_bell_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_blue_butterfly_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_liquid_galaxy_droplets_keyboard_',
    'com.cootek.smartinputv5.skin.keyboard_theme_halloween_night_keyboard_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_red_rose_sparkling',
    'com.cootek.smartinputv5.skin.keyboard_theme_blue_cool_diamond',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_flower_garden_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_water_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_green_black_metal',
    'com.cootek.smartinputv5.skin.keyboard_theme_two_lovely_bears_read_and_study',
    'com.cootek.smartinputv5.skin.keyboard_theme_dreamy_unicorn',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_live_golden_eiffel_tower',
    'com.cootek.smartinputv5.skin.keyboard_theme_blue_water_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_3d_neon_sparkling_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_cute_halloween',
    'com.cootek.smartinputv5.skin.keyboard_theme_the_holy_bible_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_hologram_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_black_tech_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_unicorn',
    'com.cootek.smartinputv5.skin.keyboard_theme_red_racing_sports_car_keyboard_theme',
    'com.cootek.smartinputv5.skin.keyboard_theme_purple_glitter_dream_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_broken_glass',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_galaxy_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_gunnery_bullet',
    'com.cootek.smartinputv5.skin.keyboard_theme_red_christmas',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_sparkle_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_live_romantic_love_heart',
    'com.cootek.smartinputv5.skin.keyboard_theme_blue_earth_galaxy',
    'com.cootek.smartinputv5.skin.keyboard_theme_simple_black_neon_green',
    'com.cootek.smartinputv5.skin.keyboard_theme_lovely_unicorn_whale',
    'com.cootek.smartinputv5.skin.keyboard_theme_bowknot_glitter_minnie_keyboard',
    'com.cootek.smartinputv5.skin.keyboard_theme_neon_hologram_racing_car',
    'com.cootek.smartinputv5.skin.keyboard_theme_golden_autumn',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_cute_kitty_lover',
    'com.cootek.smartinputv5.skin.keyboard_theme_3d_hologram_galaxy_keyboard_theme',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_flower',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_happy_emoji',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_galaxy_s8',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_heart_rose_bear',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_boom_text',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_cute_turkey2',
    'com.cootek.smartinputv5.sticker.keyboard_sticker_halloween_sticker',
    'com.cootek.smartinputv5.skin.keyboard_theme_pink_cat',
]

# appsflyer matrix plugin list
ua_maxtrix_plugin_package_name_list = [
    'com.color.call.flash.colorphone.theme.coolled',
    'com.color.call.flash.colorphone.theme.colorfulpapers',
    'com.color.call.flash.colorphone.theme.roselove',
    'com.color.call.flash.colorphone.theme.shimmering',
    'com.color.call.flash.colorphone.theme.rotatingaperture',
    'com.color.call.flash.colorphone.theme.cupidarrow',
]

gct_package_name_list = [
    "veeu.watch.funny.video.vlog.moment",
]

for app in matrix_package_name_list + ua_plugin_package_name_list + gct_package_name_list + ua_maxtrix_plugin_package_name_list:
    if app not in app2package:
        app2package[app] = app

def etl_package_name(app_name, package_name):
    if package_name and package_name.strip() != '':
        return package_name
    if app_name in app2package:
        return app2package[app_name]
    else:
        return package_name

def etl_upgrade_type(channel_code, pre_channel_code, recommend_channel):
    # old version
    if not recommend_channel:
        recommend_channel = channel_code
    if recommend_channel and recommend_channel.startswith('OEM'):
        if channel_code and channel_code.startswith('OEM'):
            return 'oem_to_oem'
        elif not pre_channel_code or pre_channel_code.startswith('OEM'):
            if channel_code.startswith('MKT'):
                return 'oem_to_mkt'
            else:
                return 'oem_to_online'
        else:
            return 'oem_to_online_to_online'
    elif recommend_channel and channel_code and channel_code.startswith('OEM'):
        return 'online_to_oem'
    else: # other condition
        return 'online_to_online'

def channel_group(channel_code, recommend_channel):
    if not recommend_channel:
        recommend_channel = channel_code
    if recommend_channel and channel_code:
        if recommend_channel.startswith('OEM'):
            if channel_code.startswith('OEM'):
                return 'oem'
            elif channel_code.startswith('MKT'):
                return 'oem_to_mkt'
            else:
                return 'oem_to_online'
        elif channel_code.startswith('OEM'):
            return 'online_to_oem'
        else:
            return 'online'
    else:
        return 'other'

# generalize some day with specified day offset, positive offset means days after this day, negative offset means days before this day
def gen_day(this_day, offset):
    this_date = datetime.strptime(this_day, '%Y%m%d')
    offset_date = timedelta(days=offset)

    some_date = this_date + offset_date
    some_day = some_date.strftime('%Y%m%d')
    
    return some_day

def gen_days_list(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y%m%d').date()
    end_date = datetime.strptime(end_date, '%Y%m%d').date()
    delta = timedelta(days=1)
    date = start_date
    days = []
    while date <= end_date:
        days.append(date.strftime('%Y%m%d'))
        date += delta
    return days

def gen_days(start_date, end_date):
    days = gen_days_list(start_date, end_date)
    str_days = '{' + ','.join(days) + '}'
    return str_days


def gen_hour(this_time, offset_hour):
    cur_format = '%Y%m%d%H'
    return (datetime.strptime(this_time, cur_format) + timedelta(hours=offset_hour)).strftime(cur_format)


def gen_hours_list(start_hour, end_hour, sep=''):
    start_hour = datetime.strptime(start_hour, '%Y%m%d%H')
    end_hour = datetime.strptime(end_hour, '%Y%m%d%H')
    delta = timedelta(hours=1)
    hour = start_hour
    hours = []
    while hour <= end_hour:
        hours.append(hour.strftime('%Y%m%d' + sep + '%H'))
        hour += delta
    return hours


def gen_process_dates(start_date, end_date, dates):
    if start_date and end_date:
        if start_date > end_date:
            raise Exception('parameter error: begin need <= end')
        return gen_days_list(start_date, end_date)
    elif dates and len(dates) > 0:
        return dates
    else:
        raise Exception('unknown dates for execution')


app_map = [(re.compile(r'cootek.smartinput.(international|mainland).android.*'), 'keyboard')]
plugin_map = [(re.compile(r'.*\.skin\..*'), 'skin'),
             (re.compile(r'.*language.*'), 'language'),
             (re.compile(r'cootek.smartinput.android.font.*'), 'font'),
             (re.compile(r'cootek.smartinput.android.emoji.*'), 'emoji'),
             (re.compile(r'cootek.smartinput.android.*touchpal.emoji.*'), 'emoji'),
             (re.compile(r'cootek.smartinput.android.sticker.keyboard_sticker.*'), 'sticker'),
             (re.compile(r'cootek.smartinput.android.celldict.*'), 'celldict'),
             (re.compile(r'com.cootek.smartinputv5.boomtextv2.*'), 'boomtext')]
matrix_plugin_map = [(re.compile(r'com\.color\.call\.flash\.colorphone\.theme\..*'), 'com.color.call.flash.colorphone.theme')]
regex_map = app_map + plugin_map + matrix_plugin_map

def app_name2bundle(app_name):
    if not app_name:
        return app_name
    for (k, v) in regex_map:
        if k.search(app_name):
            return v
    return app_name


def plugin_bundle(app_name):
    if not app_name:
        return app_name
    for (k, v) in plugin_map + matrix_plugin_map:
        if k.search(app_name):
            return v
    return app_name


# TO BE REMOVED WITH FDAU
def is_plugin(app_name):
    if not app_name:
        return False
    for (k, v) in plugin_map:
        if k.search(app_name):
            return True
    return False

def is_keyboard(app_name):
    for (k,v) in app_map:
        if k.search(app_name) and v == 'keyboard':
            return True
    return False


MKT_RE_EXP = r'^(OEM|MKT|SCD|NET)(\s[0-9A-Z]{3}){3}$'
MKT_RE = re.compile(MKT_RE_EXP)
def clean_appsflyer_channel(source_type, media_source, channel):
    channel_whitelist = ['SC000000']
    media_source_whitelist = ['Facebook Ads', 'googleadwords_int']
    if source_type != "appsflyer":
        return channel
    elif channel in channel_whitelist:
        return channel
    elif media_source in media_source_whitelist:
        return '000000'
    elif MKT_RE.match(channel):
        return channel
    else:
        return '000000'


def disabiguous_appsflyer_source(df, keys):
    from pyspark.sql.functions import col, when, regexp_replace, udf
    udf_clean_appsflyer_channel = udf(clean_appsflyer_channel)

    #replace campaign_id by campaign_name need to check when cost data include other source than Google or FB
    return df.withColumn('channel_code', udf_clean_appsflyer_channel(col('source_type'), col('media_source'), col('channel_code')))\
        .withColumn('recommend_channel', udf_clean_appsflyer_channel(col('source_type'), col('media_source'), col('recommend_channel')))\
        .withColumn('campaign', when(col('source_type') == "appsflyer", col('campaign')).otherwise('none'))\
        .withColumn('campaign_id', when((col('campaign_id').isNull()) | (col('campaign_id') == 'none'), col('campaign')).otherwise(col('campaign_id')))\
        .withColumn('campaign_id', when(col('source_type') == "appsflyer", col('campaign_id')).otherwise('none'))\
        .withColumn('partner', when(col('source_type') == "appsflyer", col('partner')).otherwise('none'))\
        .withColumn('media_source', when(col('source_type') == "appsflyer", col('media_source')).otherwise('none'))\
        .withColumn('app_name', when(col('source_type') == "appsflyer", regexp_replace('app_name','cootek.smartinput.mainland.android.public', \
            'cootek.smartinput.international.android.public')).otherwise(col('app_name')))\
        .withColumn('app_name', when(col('source_type') == "appsflyer", regexp_replace('app_name', 'cootek.smartinput.international.android.publicoem.1505',
            'cootek.smartinput.international.android.public.1505')).otherwise(col('app_name')))\
        .fillna('-', ['country'])\
        .fillna('none', subset=keys)

def etl_recommend_channel(df):
    from pyspark.sql.functions import col, when, regexp_replace

    return df.withColumn('recommend_channel', when((col('recommend_channel').isNotNull()) & (col('recommend_channel') != ''), col('recommend_channel')).otherwise(col('channel_code')))\



def get_ua_account_map(spark, start_date, end_date):
    sc = spark.sparkContext
    sc.addFile('file://' + os.path.join(os.path.dirname(__file__), 'config.py'))
    import config
    from pyspark.sql import Window
    from pyspark.sql.functions import col, row_number
    account_window = Window.partitionBy(['date', 'account_id']).orderBy(col('ad_partner').desc())
    db = 'aladdin'
    table = 'ua_ad_account_his'
    account_map = create_mysql_dataframe(spark, config.MYSQL_UA_INFO, db, table) \
        .where((col('is_deleted') == 0) & (col('date') >= start_date) & (col('date') <= end_date)) \
        .withColumnRenamed('ad_account_id', 'account_id') \
        .withColumn('rn', row_number().over(account_window)).where(col('rn') == 1).drop('rn') \
        .select(['date', 'account_id', 'ad_partner', 'owner', 'risk_level'])
    return account_map


def get_campaign_info(spark, start_date, end_date):
    sc = spark.sparkContext
    sc.addFile('file://' + os.path.join(os.path.dirname(__file__), 'config.py'))
    import config
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col, udf, lit
    from pyspark.sql.types import StringType, ArrayType

    campaign_db = 'ime_campaign'
    campaign_table = 'ime_user_acquire'
    udf_gen_days_list = udf(lambda: gen_days_list(start_date, end_date), ArrayType(StringType()))
    campaign_info = create_mysql_dataframe(spark, config.MYSQL_US1, campaign_db, campaign_table) \
        .withColumn('date_list', udf_gen_days_list()) \
        .withColumn('date', func.explode('date_list')).drop('date_list')

    return campaign_info.join(get_ua_account_map(spark, start_date, end_date), on=['date', 'account_id'], how='left_outer')\
        .select(['date', 'campaign_id', 'account_id', 'ad_partner', col('campaign_name').alias('campaign')])


def get_db_business_unit_info():
    import config
    import MySQLdb
    ### mysql config
    db_conf = {
        'user': config.MYSQL_APP_INFO['user'],
        'passwd': config.MYSQL_APP_INFO['password'],
        'host': config.MYSQL_APP_INFO['host'],
        'db': config.MYSQL_APP_INFO['db']
    }
    db = MySQLdb.connect(**db_conf)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    app_bu_info = {}
    ### query
    for bu in config.PATH_DICT.keys():
        table_name = 'app_info_%s' % bu
        sql = 'SELECT app_name, product_group from %s where LOWER(product_group) = %s' % (table_name, '%s')
        params = (bu,)
        cur.execute(sql, params)
        for row in cur.fetchall():
            if row['app_name'] in app_bu_info:
                raise Exception('duplicate app_name %s' % row['app_name'])
            app_bu_info[row['app_name']] = row['product_group']
    ### close mysql
    cur.close()
    db.close()
    return app_bu_info


def get_business_unit(app_name, app_bu_info):
    if app_name in app_bu_info:
        return app_bu_info[app_name].upper()
    else:
        # default business unit is gb
        return 'GB'


APP_PATH_DICT = [
    (re.compile(r'^clash\.crush\.fun\.emoji\.game.*$'), 'clash.crush.fun.emoji.game'),
    (re.compile(r'^com\.cootek\.smartinputv5.*$'), 'smartinput'),
    (re.compile(r'^cootek\.smartinput\.(android|international|mainland).*$'), 'smartinput'),
]


def get_app_path(app_name, app_bu_info):
    if not app_name:
        return 'none'
    for (k, v) in APP_PATH_DICT:
        if k.search(app_name):
            return v
    if app_name in app_bu_info:
        return app_name
    else:
        return 'none'


## referrer parser
other_skins=["luklek",
       "freshlight",
       "1upthemes",
       "alphathemes",
       "elegantthemes",
       "gradientthemes",
       "keyboardemojithemes",
       "newthemes",
       "omegathemes",
       "sublimethemes",
       "tildekeyboards",
       ]

is_noah_re = re.compile(r'''(?x)
        ime_push_notification
        |ime_drawer_notification
        '''
        )

def parse_others(input_str):
    for key in other_skins:
        if key in input_str:
            return "skin."+key
    return ''


def is_noah(input_str):
    if is_noah_re.search(input_str):
        return True
    else:
        return False


valid_utm_type_re = re.compile(r'(?i)^(OPS|SEO|SKIN|EMOJI|FONT|STICKER|LANGUAGE|CELLDICT|BOOMTEXT)$')


def is_valid_utm_type(input_str):
    if input_str and isinstance(input_str, (str, unicode)) and valid_utm_type_re.search(input_str):
        return True
    else:
        return False


is_possible_ops_re = re.compile(r'(?x)utm_source=\w+|utm_campaign=\w+|utm_medium=\w+|utm_type=\w+')
def is_possible_ops(input_str):
    if input_str and isinstance(input_str, (str, unicode)) and is_possible_ops_re.search(input_str):
        return True
    else:
        return False

priority = {
    'skin': 1,
    'emoji': 1,
    'font': 1,
    'sticker': 1,
    'language': 1,
    'celldict': 1,
    'boomtext': 1,
    'noahpush': 1,
    'ops': 1,
    'seo': 1,
    'google_organic': 1,
    'possible_ops': 2,
    'possible_appsflyer': 2
}

def getReferrer(referrer):
    import base64
    raw_referrer = referrer
    if referrer is None or len(referrer) == 0:
        return None

    utm_source = 'utm_source='
    utm_campaign = 'utm_campaign='
    utm_medium = 'utm_medium='
    utm_type = 'utm_type='
    utm_src_android_id = 'utm_src_android_id='
    utm_src_gaid = 'utm_src_gaid='
    utm_src_uuid = 'utm_src_uuid='
    utm_src_referrer = 'utm_src_referrer='
    utm_src_app_name = 'utm_src_app_name='

    utm_dict = {utm_source: None,
                utm_campaign: None,
                utm_medium: None,
                utm_type: None,
                utm_src_android_id: None,
                utm_src_gaid: None,
                utm_src_uuid: None,
                utm_src_referrer: None,
                utm_src_app_name: None
                }

    referrer_list = referrer.split('&')
    for item in referrer_list:
        for key in utm_dict:
            if item.startswith(key):
                utm_dict[key] = item[len(key):]

    # check case referrer with source activate info
    # decode src referrer
    if utm_dict[utm_src_referrer]:
        # decode if src referrer was encoded
        try:
            utm_dict[utm_src_referrer] = base64.b64decode(utm_dict[utm_src_referrer])
        except Exception as e:
            pass
    # get src app name
    if (utm_dict[utm_src_gaid] or utm_dict[utm_src_android_id]) and (utm_dict[utm_source] == 'theme') and (utm_dict[utm_src_app_name] is None):
        utm_dict[utm_src_app_name] = utm_dict[utm_campaign]

    # utm_type
    # uppercase and lowercase utm_type will be accepted as source_type and transformed to lowercase
    if is_valid_utm_type(utm_dict[utm_type]):
        return utm_dict[utm_type].lower(), priority[utm_dict[utm_type].lower()], utm_dict[utm_source], utm_dict[utm_campaign], utm_dict[utm_medium], utm_dict[utm_src_app_name], utm_dict[utm_src_android_id], utm_dict[utm_src_gaid], utm_dict[utm_src_uuid], utm_dict[utm_src_referrer]

    # noah
    if utm_dict[utm_source] and is_noah(utm_dict[utm_source]):
        return 'noahpush', priority['noahpush'], utm_dict[utm_source], utm_dict[utm_campaign], utm_dict[utm_medium], utm_dict[utm_src_app_name], utm_dict[utm_src_android_id], utm_dict[utm_src_gaid], utm_dict[utm_src_uuid], utm_dict[utm_src_referrer]

    # from google play store
    if referrer == "utm_source=google-play&utm_medium=organic":
        return 'google_organic', priority['google_organic'], utm_dict[utm_source], utm_dict[utm_campaign], utm_dict[utm_medium], None, None, None, None, None


    # default referrer logic
    types = ['theme', 'emoji', 'font', 'sticker', 'language', 'cell_dict', 'boom_text']
    is_ok = False
    for tp in types:
        if 'utm_source=%s' % tp in referrer and 'utm_campaign=' in referrer and '&utm_medium=' in referrer:
            referrer = referrer[referrer.find('utm_campaign=')+13:referrer.find('&utm_medium=')]
            is_ok = True
            break
        elif 'utm_medium=%s' % tp in referrer and 'utm_source' in referrer and 'utm_campaign' in referrer:
            source = 'utm_source='
            referrer = referrer[referrer.find(source) + len(source):referrer.find('&utm_campaign=')]
            is_ok = True
            break

    if not is_ok:
        if '_utm_source' in referrer:
            referrer = referrer.split('_utm_source')[0]
        elif 'utm_source' in referrer:
            referrer = referrer.split('__')[1] if '__' in referrer else referrer
            referrer = referrer.split('&')[0] if '&' in referrer else referrer
        elif '_source' in referrer:
            referrer = referrer.split('_source')[0]

        for item in ['&','_com.','_click','_OEM','_9apps','_TPstore','_tpstore',]:
            if item in referrer:
                referrer = referrer.split(item)[0]
                break

    others = parse_others(referrer)
    if others:
        referrer=others
    elif len(referrer)>0 and referrer[-1]=='_':
        referrer=referrer[:-1]
    if 'com.cootek.smartinputv5' in referrer:
        referrer = referrer[referrer.find('com.cootek.smartinputv5')+24:]
    elif 'com.cootek.smartinput' in referrer:
        referrer = referrer[referrer.find('com.cootek.smartinput')+22:]

    for kw in ['skin', 'emoji', 'font', 'sticker', 'language', 'celldict', 'boomtext']:
        if kw in referrer:
            return kw, priority[kw], referrer.strip(), None, utm_dict[utm_medium], utm_dict[utm_src_app_name], utm_dict[utm_src_android_id], utm_dict[utm_src_gaid], utm_dict[utm_src_uuid], utm_dict[utm_src_referrer]

    ## possible type
    if is_possible_ops_re.search(raw_referrer):
        return 'possible_ops', priority['possible_ops'], utm_dict[utm_source], utm_dict[utm_campaign], utm_dict[utm_medium], utm_dict[utm_src_app_name], utm_dict[utm_src_android_id], utm_dict[utm_src_gaid], utm_dict[utm_src_uuid], utm_dict[utm_src_referrer]
    else:
        return 'possible_appsflyer', priority['possible_appsflyer'], utm_dict[utm_source], utm_dict[utm_campaign], utm_dict[utm_medium], utm_dict[utm_src_app_name], utm_dict[utm_src_android_id], utm_dict[utm_src_gaid], utm_dict[utm_src_uuid], utm_dict[utm_src_referrer]


__bracket_re = re.compile(r'''(?x)
    \{.*\}
    |\[.*\]
    |\(.*\)
    ''')

__filter_path_re = re.compile(
        r'''(?x)
        /STATISTIC/NETWORK/CMD_REQUEST
        |/PERFORMANCE/PERF_COLLECT
        |/PERFORMANCE/PERF_CORE
        |/STATISTIC/\w+_(tplus)?azerty\d*_
        |/STATISTIC/\w+_(tplus)?phonepad\d*_
        |/STATISTIC/\w+_(tplus)?qwerty\d*_
        |/STATISTIC/\w+_(tplus)?qwertz\d*_
        |/STATISTIC/\w+_tplus_
        |/STATISTIC/\w+_cu1_
        |/STATISTIC/\w+_custom1_
        |/STATISTIC/\w*_handwrite_
        |/STATISTIC/\w+_bihua_
        '''
    )

__illegal_path_re = re.compile(r'''[^/_a-zA-Z0-9]''')

def decode_usage_content(cont, path_white_list=None):
    ret = []
    template = {}
    for key,val in cont.items():
        if key != 'request':
            template[key] = val

    if 'data' not in cont['request']:
        return ret

    for data in cont['request']['data']:
        if 'path' not in data or __filter_path_re.search(data['path']):
            continue
        if __illegal_path_re.search(data['path']):
            logging.warning('error_dir: %s' % data['path'])
            continue
        if path_white_list and data['path'].replace('/', '_').replace('.', '_') not in path_white_list:
            continue
        #assume only one level
        this_one = copy.copy(template)
        this_one['path'] = data['path']
        if 'value' not in data:
            continue
        try:
            value_d = json.loads(data['value'])
            if isinstance(value_d, int):
                raise Exception('value_d has invalid type')
        except Exception as e:
            logging.warning('json load failed2: %s' % (data['value']))
            continue
        if 'timestamp' in value_d:
            try:
                value_d['timestamp'] = datetime.utcfromtimestamp(int(value_d['timestamp'])//1000).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logging.warning('timestamp convert failed: %s' % str(value_d['timestamp']))
                continue
        if 'timezone' in value_d:
            value_d['timezone'] = str(value_d['timezone'])

        #NOTE: at most drill 2 depth
        need_update = {}
        for key, val in value_d.items():
            if not isinstance(val, str) or not __bracket_re.search(val):
                continue
            try:
                cont2 = json.loads(val)
            except ValueError:
                continue
            need_update[key] = cont2
        if need_update:
            value_d.update(need_update)

        #remove western.usr content
        if this_one['path'] == '/STATISTIC/DICT_RECOVERY/USER_DICT_CHECK':
            if 'content_western.usr' in value_d:
                value_d['content_western.usr'] = 1
            else:
                value_d['\/STATISTIC\/DICT_RECOVERY\/USER_DICT_CHECK'] = 0
        elif this_one['path'].startswith('/STATISTIC/DETAIL_PERF/') and 'perf' in value_d and not isinstance(value_d['perf'], dict):
            continue

        this_one['value_d'] = value_d
        ret.append(this_one)
    return ret

def diff_days(start_date, end_date):
    if start_date and end_date and (start_date <= end_date):
        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')
        return (end_date-start_date).days + 1
    else:
        return -1

def diff_months(start_month, end_month):
    if start_month and end_month and (start_month <= end_month):
        start_date = datetime.strptime(start_month + '01', '%Y%m%d')
        end_date = datetime.strptime(end_month + '01', '%Y%m%d')
        return relativedelta.relativedelta(end_date, start_date).months + 1
    else:
        return -1

def diff_weeks(start_date, end_date):
    if start_date and end_date and (start_date <= end_date):
        start_day = datetime.strptime(start_date, '%Y%m%d')
        end_day = datetime.strptime(end_date, '%Y%m%d')
        return (end_day - start_day).days / 7 + 1
    else:
        return -1

def diff_n(act, active, retention_type):
    import config
    if retention_type == config.RETENTION_TYPE_DAILY:
        return diff_days(act, active)
    elif retention_type == config.RETENTION_TYPE_MONTHLY:
        return diff_months(act, active)
    elif retention_type == config.RETENTION_TYPE_WEEKLY:
        return diff_weeks(act, active)
    else:
        return ''


def get_roi_dataframe(spark, activate_path, revenue_path, launch_active_path, bg_active_path, cost_path, group_keys=None, filter='1 = 1', app_bundle_func=plugin_bundle):
    if not group_keys:
        group_keys = ROI_KEYS
    from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, ArrayType, StringType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col, row_number, when, udf, count, collect_list, regexp_replace, first

    udf_diff_days = udf(diff_days, IntegerType())
    udf_app_bundle = udf(lambda n: app_bundle_func(n))

    def udf_explode_diff_days(end_date):
        return udf(lambda day: range(1, diff_days(day, end_date) + 1), ArrayType(IntegerType()))

    activate = spark.read.parquet(activate_path).where(filter)\
        .withColumn('act_date', col('act_time').substr(1, 8))\
        .withColumnRenamed('act_country', 'country')\
        .withColumnRenamed('campaign_name', 'campaign')
    activate = disabiguous_appsflyer_source(activate, ROI_KEYS)\
        .groupBy(ROI_KEYS + ['act_date']).agg(func.count('*').alias('activate'))\
        .withColumn('app_name', udf_app_bundle(col('app_name')))\
        .groupBy(ROI_KEYS + ['act_date']).agg(func.sum('activate').alias('activate')).cache()

    active_keys = format_roi_keys(group_keys, ['act_date', 'date', 'n'], [])
    revenue = spark.read.parquet(revenue_path).where(filter)\
        .withColumn('act_date', col('act_time').substr(1, 8))\
        .withColumnRenamed('campaign_name', 'campaign')\
        .withColumn('country', when(col('country') == '-', col('act_country')).otherwise(col('country')))
    revenue = disabiguous_appsflyer_source(revenue, ROI_KEYS)\
        .groupBy(ROI_KEYS + ['act_date', 'date']).agg(func.sum('revenue').alias('revenue'))\
        .withColumn('app_name', udf_app_bundle(col('app_name')))\
        .withColumn('n', udf_diff_days(col('act_date'), col('date')))\
        .groupBy(active_keys).agg(func.sum('revenue').alias('revenue'))

    active = None
    for active_path, dau in {launch_active_path: 'launch_active', bg_active_path: 'bg_active'}.items():
        tmp_active = spark.read.parquet(active_path).where(filter)\
            .withColumn('act_date', col('act_time').substr(1, 8))\
            .withColumn('country', when(col('act_country').isNull() | (col('act_country') == "-"), col('ip_city')).otherwise(col('act_country')))
        tmp_active = disabiguous_appsflyer_source(tmp_active, ROI_KEYS)\
            .groupBy(ROI_KEYS + ['act_date', 'date']).agg(func.count('*').alias(dau))\
            .withColumn('app_name', udf_app_bundle(col('app_name')))\
            .withColumn('n', udf_diff_days(col('act_date'), col('date')))\
            .groupBy(active_keys).agg(func.sum(dau).alias(dau))
        active = active.join(tmp_active, on=active_keys, how='outer') if active else tmp_active
    # plugin's kdau is not reliable use tdau instead
    active = active.withColumn('retention', when(col('app_name').isin(ua_plugin_package_name_list), col('bg_active')).otherwise(col('launch_active')))\
        .where(col('retention').isNotNull() & (col('retention') > 0)).cache()

    # tdau might have dirty date info in data, use date from kdau
    end_active_day = active.where('launch_active is not null').agg(func.max('date').alias('max_date')).first()['max_date']
    end_activate_day = activate.agg(func.max('act_date').alias('max_act_date')).first()['max_act_date']
    logging.info('last active date is %s, activate date is %s' % (end_active_day, end_activate_day))

    retention = activate.withColumn('n_list', udf_explode_diff_days(end_active_day)(col('act_date')))\
        .withColumn('n', func.explode(col('n_list'))).drop('n_list')\
        .groupBy(format_roi_keys(group_keys, ['act_date', 'n'], [])).agg(func.sum('activate').alias('activate'))\
        .join(active, on=format_roi_keys(group_keys, ['act_date', 'n'], []), how='left_outer').fillna(0, 'retention').drop('date')

    cost_keys = ["act_date", "country", "campaign_id"]
    activate_for_cost = activate.groupBy(cost_keys).agg(func.sum('activate').alias('activate'), func.count('*').alias('count'))

    cost = spark.read.parquet(cost_path).withColumnRenamed('date', 'act_date').groupBy(cost_keys)\
        .agg(func.sum('spend').alias('cost_spend'), func.sum('activate').cast('int').alias('cost_activate'))\
        .join(activate_for_cost, on=cost_keys, how='outer')\
        .fillna('-', ['country']).fillna(0, 'activate')\
        .withColumn('cost', when(col('activate') == 0, col("cost_spend")).otherwise(col("cost_spend") / col('activate'))).drop('activate')\
        .join(activate, on=cost_keys, how='outer').fillna(0, 'activate')\
        .withColumn('total_cost', when(col('activate') == 0, col('cost_spend')).otherwise(col('cost') * col('activate')))\
        .groupBy(group_keys).agg(func.sum(col('total_cost')).alias('cost_spend'),
                                 func.sum('cost_activate').alias('cost_activate'), func.sum('activate').alias('activate'))\
        .fillna('none', subset=group_keys).fillna(0, 'activate')\
        .withColumn('cost', when(col('activate') == 0, func.lit(0)).otherwise(col("cost_spend") / col('activate')))

    return revenue, retention, cost, end_activate_day, end_active_day

def format_roi_keys(key, add, sub):
    res = key[:] + [item for item in add if item not in key]
    res = [item for item in res if item not in sub]
    return res




def ip2loc(ip_db, ip):
    country = '-'
    try:
        if ip and ',' in ip:
            ip = ip.split(',')[0].strip()
        country = ip_db.get_country_short(ip)
    except Exception as e:
        logging.error(e)
    return country


ip_db = None


def geoip2loc(ip_db_path, ip):
    country = '-'
    try:
        if ip and ',' in ip:
            ip = ip.split(',')[0].strip()
        global ip_db
        if not ip_db:
            import geoip2.database
            ip_db = geoip2.database.Reader(ip_db_path)
        country = ip_db.city(ip).country.iso_code
    except Exception as e:
        logging.error(e)
    return country


def upgrade_type(old_act_app_version, act_app_version, app_version):
    if not old_act_app_version and not act_app_version:
        return 'unknown'
    elif not act_app_version and old_act_app_version:
        act_app_version = old_act_app_version
    try:
        act_app_version_int = int(act_app_version)
        app_version_int = int(app_version)
        if act_app_version_int < app_version_int:
            return 'upgrade'
        elif act_app_version_int == app_version_int:
            return 'new'
        elif act_app_version_int > app_version_int:
            return 'unknown'
    except Exception as e:
        return 'unknown'


def add_missing_column(df, col, default_value=None, default_type='string'):
    from pyspark.sql.functions import lit
    if col not in df.columns:
        return df.withColumn(col, lit(default_value).cast(default_type))
    else:
        return df


def add_missing_source_column(df):
    missing_cols = ['activate_type', 'channel', 'site_id', 'utm_source', 'utm_campaign', 'utm_medium', 'osv', 'uuid', 'ad_type', 'ad_id', 'ad', 'adset_id', 'adset']
    for col in missing_cols:
        df = add_missing_column(df, col)

    return df


# 'BY 4.3| Android: 4.4.2' --> 4.3
# 'iOS 11.3' --> 11.3
# '10.0.214A456' --> None
OSV_RE = re.compile(r'^(?:.*?[^0-9\.])?(\d{1,2}(?:\.\d){1,2})(?:[^0-9\.].*)?$')


def clean_osv_format(osv):
    if osv:
        s = OSV_RE.match(osv)
        if s:
            return s.group(1)
    return None


# check if osv is in valid format, replace with null if not valid
# check if osv is null, fill with none if null
def check_valid_osv(df, fillna=False):
    from pyspark.sql.functions import col, udf
    udf_clean_osv_format = udf(clean_osv_format)
    df = df.withColumn('osv', udf_clean_osv_format(col('osv')))
    if fillna:
        df = df.fillna('none', 'osv')
    return df


# adapt read dataframe from json, parquet hdfs file or hive using select
def load_dataframe(spark, path):
    sql_re = re.compile(r'(?i)^SELECT\s')
    if sql_re.match(path):
        return spark.sql(path)
    elif '/json/' in path:
        return spark.read.json(path)
    elif '/parquet/' in path:
        return spark.read.parquet(path)
    else:
        raise Exception('unknown input path %s' % path)


def create_activate_source_dataframe(spark, activate_source_path, activate_source_summary_path):
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import col, row_number, when, udf
    from pyspark.sql.types import StringType

    udf_type_name = udf(etl_type_name, StringType())

    cols = ['identifier', 'act_time', 'act_country', col('app_name').alias('act_app_name'), 'android_id', 'uuid', col('app_version').alias('act_app_version')
            , col('channel_code').alias('act_channel_code'), col('recommend_channel').alias('act_recommend_channel')
            , 'media_source', 'partner', 'channel', 'site_id', 'campaign_id', 'campaign', 'referrer', 'source_type',
            'utm_source', 'utm_campaign', 'utm_medium', 'osv', 'ad_type', 'ad_id', 'ad', 'adset_id', 'adset']

    activate_source = None
    for path in [activate_source_summary_path, activate_source_path]:
        if path:
            df = spark.read.option("mergeSchema", "true").parquet(path)
            df = add_missing_source_column(df).select(cols)
            activate_source = activate_source.union(df) if activate_source else df

    if not activate_source:
        raise Exception('activate data not found')

    window = Window.partitionBy(col('identifier'), col('type_name')).orderBy(col('act_time').desc())
    return activate_source.withColumn('type_name', udf_type_name(col('act_app_name'), col('act_channel_code'), col('act_recommend_channel')))\
        .fillna('none', subset=['type_name'])\
        .withColumn('rn', row_number().over(window)).where(col('rn') == 1).drop('rn')\
        .withColumn('act_date', col('act_time').substr(1, 8))


def add_old_activate_info(spark, df, old_activate_summary):
    from pyspark.sql.functions import col, udf, lit, coalesce
    from pyspark.sql.types import StringType
    udf_upgrade_type = udf(upgrade_type, StringType())

    if old_activate_summary:
        old_activate_source = create_activate_source_dataframe(spark, None, old_activate_summary)\
            .select('identifier', 'type_name', col('act_app_version').alias('old_act_app_version'), col('osv').alias('old_osv'))
        df = df.join(old_activate_source, on=['identifier', 'type_name'], how='left_outer')\
            .withColumn("osv", coalesce(col('osv'), col('old_osv'))).drop('old_osv')
    else:
        df = df.withColumn('old_act_app_version', lit(''))
    return df.withColumn('activate_type', udf_upgrade_type(col('old_act_app_version'), col('act_app_version'), col('app_version')))\
        .drop('old_act_app_version')


def join_activate_source(spark, activate_source_path, activate_source_summary_path, df, old_activate_summary=None):
    from pyspark.sql.functions import col, udf, lit
    from pyspark.sql.types import StringType

    udf_type_name = udf(etl_type_name, StringType())

    activate_source = create_activate_source_dataframe(spark, activate_source_path, activate_source_summary_path)

    df = df.withColumn('type_name', udf_type_name(col('app_name'), col('channel_code'), col('recommend_channel')))\
        .fillna('none', subset=['type_name'])\
        .join(activate_source, on=['identifier', 'type_name'], how='left_outer')\
        .fillna('-', 'country')
    df = add_old_activate_info(spark, df, old_activate_summary)

    return df.drop('type_name')

        
def create_mysql_dataframe(spark, server, db, tb, cols=None):
    url = 'jdbc:mysql://%s' % server['host']
    params = {"driver": "com.mysql.jdbc.Driver", 'user':server['user'], 'password':server['password']}
    
    df = spark.read.jdbc('%s/%s' % (url, db), tb, properties=params)
    df = df.select(cols).cache() if cols else df.cache()
    
    if df.count()==0:
        raise Exception('mysql table data exception ---- %s:%s' % (db, tb))
    return df


def check_df_unique_key(df, keys):
    df_unique = df.groupBy(keys).count().where('count > 1')
    if df_unique.count():
        raise ValueError('duplicate %s %s' % (keys, df_unique.collect()))


def etl_source_type(source_type, recommend_channel, channel_code):
    if source_type is None \
        and ((recommend_channel and recommend_channel.startswith('OEM'))\
            or (channel_code and channel_code.startswith('OEM'))):
        return 'oem'
    else:
        return source_type


def get_traffic_name(traffic_type_code):
    traffic_type_code_map = {
        1: 'private_access',
        2: 'auction',
        3: 'remaining',
        4: 'sniper_network'
    }

    if traffic_type_code not in traffic_type_code_map.keys():
        return None
    return traffic_type_code_map[traffic_type_code]


def create_placementid_map(spark, db_conf, this_date):
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    udf_treffic_name = udf(get_traffic_name, StringType())
    # mysql config
    db = 'ime_config'
    table = 'ime_advertise_tu_placement_map_his'
    placementid_to_tu = create_mysql_dataframe(spark, db_conf, db, table)\
        .where(col('date') == this_date)\
        .select(col('date'), col('placement_id').alias('placementid'), col('tu').cast('string'), col('tu_name'),
                col('app_name'), col('bundle_id').cast('string'), col('bundle_name'), col('id_type'),
                col('exp_id').cast('string'), col('ad_config_name'), col('ad_config_id').cast('string'),
                col('sub_group').cast('string'), col('group').cast('string'), col('exp_name'),
                col('exp_group_id').cast('string'), col('traffic_type')) \
        .withColumn('traffic_name', udf_treffic_name(col('traffic_type'))).cache()

    # check placementid is unique in a day
    check_df_unique_key(placementid_to_tu, ['placementid', 'date'])
    return placementid_to_tu


def create_real_time_placementid_map(spark, db_conf):
    from pyspark.sql.functions import col, row_number, lit
    from pyspark.sql import Window

    db = 'hades_zh'
    table = 'ime_advertise_tu_placement_map'
    placementid_to_tu = create_mysql_dataframe(spark, db_conf, db, table) \
        .select(col('placement_id').alias('placementid'), col('tu').cast('string'),
                col('update_at'), col('app_name'), col('id_type'))
    placement_window = Window.partitionBy(col('placementid'), col('tu')).orderBy(col('update_at').desc())
    placementid_to_tu = placementid_to_tu.distinct() \
        .withColumn('rn', row_number().over(placement_window)).where(col('rn') == 1).drop('rn').drop('update_at').cache()
    tu_table = 'tu'
    tu_window = Window.partitionBy(col('tu'), col('tu_name')).orderBy(col('update_at').desc())
    tu_to_name_config = create_mysql_dataframe(spark, db_conf, db, tu_table) \
        .withColumnRenamed('name', 'tu_name')\
        .select(col('update_at'), col('id').cast('string').alias('tu'), col('tu_name'), col('bundle_id'),
                col('bundle_name')).distinct()
    tu_to_name = tu_to_name_config.withColumn('rn', row_number().over(tu_window)) \
        .where(col('rn') == 1).drop('rn').drop('update_at').cache()
    placementid_to_tu = placementid_to_tu.join(tu_to_name, on=['tu'], how='left_outer')
    return placementid_to_tu


def create_tu_to_name(spark, db_conf, this_date):
    import pyspark.sql.functions as func
    from pyspark.sql.functions import col, udf, row_number
    from pyspark.sql import Window

    db = 'ime_config'
    placement_table = 'ime_advertise_tu_placement_map_his'
    tu_table = 'tu'

    tu_to_name_placement = create_mysql_dataframe(spark, db_conf, db, placement_table)\
        .where(col('date') == this_date)\
        .select(col('date'), col('tu').cast('string'), col('tu_name'), col('bundle_id'), col('bundle_name'), func.lit(1).alias('priority')).distinct()

    tu_db = 'hades_zh'
    tu_to_name_config = create_mysql_dataframe(spark, db_conf, tu_db, tu_table) \
        .withColumnRenamed('name', 'tu_name') \
        .withColumn('date', func.lit(this_date))\
        .select(col('date'), col('id').cast('string').alias('tu'), col('tu_name'), col('bundle_id'), col('bundle_name'), func.lit(2).alias('priority')).distinct()
    tu_window = Window.partitionBy(col('date'), col('tu')).orderBy(col('priority'))
    tu_to_name = tu_to_name_placement.union(tu_to_name_config).distinct()\
        .withColumn('rn', row_number().over(tu_window)).where(col('rn') == 1).drop('rn').drop('priority')\
        .cache()
    # check bundle and tu name is unique for tu in a day
    check_df_unique_key(tu_to_name, ['tu', 'date'])
    bundle_to_name = tu_to_name.select('date', 'bundle_id', 'bundle_name').distinct().cache()
    check_df_unique_key(bundle_to_name, ['bundle_id', 'date'])
    return tu_to_name


campaign_update_cols = ['app_name', 'recommend_channel', 'channel_code', 'media_source', 'partner']


def join_campaign_info(df, campaign_info):
    from pyspark.sql.functions import col, when
    df = df.join(campaign_info, on=['source_type', 'campaign_id'], how='left_outer')
    for c in campaign_update_cols:
        new_c = 'campaign_%s' % c
        df = df.withColumn(c, when(col(new_c).isNotNull() & col(c).isNull(), col(new_c)).otherwise(col(c)))
    return df


# exclude appsflyer skin product from ordinary skin plugin
def ua_plugin_bundle(app_name):
    if app_name in ua_plugin_package_name_list + ua_maxtrix_plugin_package_name_list:
        return app_name
    else:
        return plugin_bundle(app_name)


MOPUB_PLACEMENT_RE = re.compile(r'^[a-z0-9]{32}$')


# update placement to update_sspid
def merge_sspid(placementid, sspid, placement2ssp):
    try:
        str_sspid = str(sspid)
    except Exception as e:
        str_sspid = ''
    if placementid in placement2ssp and placement2ssp[placementid]:
        return placement2ssp[placementid]
    elif placementid and str_sspid == '4' and MOPUB_PLACEMENT_RE.match(placementid):
        # fix app issue which send mopub placement with sspid 4
        return 5
    else:
        return sspid


def update_sspid(spark, df, sspid_config_path):
    from pyspark.sql.functions import col, when, udf
    local_placement2ssp = spark.read.json(sspid_config_path).rdd.collectAsMap()
    placement2ssp = spark.sparkContext.broadcast(local_placement2ssp)
    udf_update_sspid = udf(lambda placementid, sspid: merge_sspid(placementid, sspid, placement2ssp.value))
    return df.withColumn('sspid', udf_update_sspid(col('placementid'), col('sspid')))

# exchange cost to USD
# parameters:
#   cost_unit: origin currency
#   cost: cost value
#   exchange_rate: exchange to USD rate dict
def exchange_cost_to_USD(cost_unit, cost, exchange_rate):
    valid_currency_format = re.compile(r'^[A-Z]{3}$')
    if valid_currency_format.match(cost_unit):
        if cost_unit in exchange_rate:
            usd_cost = float(cost) * exchange_rate[cost_unit]
            return usd_cost
        else:
            raise Exception("ERROR: unknown currency %s" % cost_unit)
    else:
        raise Exception("ERROR: invalid currency format %s" % cost_unit)

# get currency exchange rate dict, from others to USD
def build_to_USD_rate(config, end_date):
    db_conf = {
        'user': config.MYSQL_DATA_CONFIG['user'],
        'passwd': config.MYSQL_DATA_CONFIG['password'],
        'host': config.MYSQL_DATA_CONFIG['host'],
        'db': 'ime_data_config'
    }
    import MySQLdb

    db = MySQLdb.connect(**db_conf)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    query = "Select * from daily_currency_exchange_rate where `date` = %s;"
    cur.execute(query, [end_date])
    rows = cur.fetchall()
    to_USD_rate = {}
    for r in rows:
        to_USD_rate[r['to']] = r['reverse_amount']
    return to_USD_rate


# build_funnels
def build_funnels(config, funnel_type='PAGEVIEW'):
    FUNNEL_TYPES = {
        'PAGEVIEW': 1,
        'SESSION': 2
    }
    funnels = {}
    db_conf = {
        'user': config.MYSQL_DATA_CONFIG['user'],
        'passwd': config.MYSQL_DATA_CONFIG['password'],
        'host': config.MYSQL_DATA_CONFIG['host'],
        'db': 'ime_data_config',
        'charset': 'utf8'
    }
    import MySQLdb

    db = MySQLdb.connect(**db_conf)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    query = "Select * from pageview_funnel_config where funnel_type=%s;" % FUNNEL_TYPES[funnel_type]
    cur.execute(query)
    rows = cur.fetchall()
    for r in rows:
        if r['app_name'] not in funnels:
            funnels[r['app_name']] = {}
        if r['funnel_name'] not in funnels[r['app_name']]:
            funnels[r['app_name']][r['funnel_name']] = {}
        if r['step_id'] not in funnels[r['app_name']][r['funnel_name']]:
            funnels[r['app_name']][r['funnel_name']][r['step_id']] = {'step_name': r['step_name'], 'page_names': r['page_names'].split(',')}
    return funnels


# add missing keys columns to be backward compatible
def add_report_columns(df, keys):
    for c in keys:
        df = add_missing_column(df, c, 'none')
        if c == 'osv':
            df = check_valid_osv(df, True)
    return df

def get_app_name(app_name_list, all_app_name_list):
    # check all app names' validity if app_name_list is not None
    if app_name_list is None:
        app_name_list = all_app_name_list
    elif len(app_name_list)==0:
        raise Exception("ERROR: app_name input empty.")
    else:
        all_app_name_set = set(all_app_name_list)
        app_name_set = set(app_name_list)
        if not app_name_set.issubset(all_app_name_set):
            invalid_app_name_set = app_name_set - all_app_name_set
            raise Exception('ERROR: invalid app name {%s}' % ','.join(invalid_app_name_set))

    return app_name_list


# time convert in different time zone
# parameters:
#   origin_time: origin time string
#   origin_format: origin time format
#   origin_time_zone: origin time zone, valid time zone in pytz
#   dest_format: destination time
#   dest_time_zone: destination time zone, considering day light time offset
def time_zone_convert(origin_time, origin_format, origin_time_zone, dest_format, dest_time_zone):
    import pytz
    origin_tzinfo = pytz.timezone(origin_time_zone)
    t = datetime.strptime(origin_time, origin_format).replace(tzinfo=origin_tzinfo)

    dest_tzinfo = pytz.timezone(dest_time_zone)
    local_time = t.astimezone(tz=dest_tzinfo)

    return local_time.strftime(dest_format)


# transform utc time to local time with given media_source
def get_local_time(media_source, utc_time, utc_format, dest_format):
    media_source_tzinfo = {
        'snapchat_int': 'America/Los_Angeles',
        'Facebook Ads': 'Asia/Shanghai',
        'googleadwords_int': 'Asia/Shanghai',
        'yahoogemini_int': 'Asia/Hong_Kong'
    }

    if media_source in media_source_tzinfo:
        dest_time_zone = media_source_tzinfo[media_source]
    else:
        dest_time_zone = 'UTC'

    return time_zone_convert(origin_time=utc_time, origin_format=utc_format, origin_time_zone='UTC', dest_format=dest_format, dest_time_zone=dest_time_zone)


# map from appsflyer app id to app name, recently only update smartinput app
# TODO: need to perfect this map info in the future
package2app = {
    'com.cootek.smartinputv5': 'cootek.smartinput.international.android.public',
    'com.emoji.keyboard.touchpal': 'cootek.smartinput.international.android.public.1505',
}
def package_name2app_name(package_name):
    if package_name in package2app:
        return package2app[package_name]
    else:
        return package_name

# extract traffic type from ad exp_tag
def get_traffic_type(exp_tag):
    if exp_tag:
        traffic_type = exp_tag.split('*^*')[0]

        return traffic_type
    else:
        return None


# when bundle name does not contain hades (case insensitive) then revenue belongs to no-hades
# other revenue like goblin has no bundle name
HADES_NAME_RE = re.compile(r'(?i)^hades$')
def is_hades_bundle(bundle_name):
    if not bundle_name or (bundle_name == 'none') or HADES_NAME_RE.search(bundle_name):
        return True
    else:
        return False


def revenue_no_hades(revenue, bundle_name):
    if is_hades_bundle(bundle_name):
        return 0.0
    else:
        return revenue


def get_activate_raw_df(spark, activate_raw_path):
    from pyspark.sql import Window
    from pyspark.sql.functions import col, lit, row_number

    activate_window = Window.partitionBy(col('identifier'), col('app_name')).orderBy(col('act_time').desc())
    activate_path_raw = spark.read.parquet(activate_raw_path).select('identifier', 'act_time', 'activate_type',
                                                                     'app_name') \
        .withColumnRenamed('activate_type', 'activate_type_this_day') \
        .withColumn('rn', row_number().over(activate_window)) \
        .where(col('rn') == 1).drop('rn').drop('act_time')
    return activate_path_raw


def join_activate_raw(df, activate_raw):
    from pyspark.sql.functions import col, when, lit
    result = df.join(activate_raw, on=['identifier', 'app_name'], how='left_outer') \
        .withColumn('is_act', when(col('activate_type_this_day').isNotNull(), lit('true')).otherwise(lit('false'))) \
        .drop('activate_type_this_day')
    return result

def join_dau_overlap(df, dau_overlap):
    from pyspark.sql.functions import col, when, lit
    if not dau_overlap:
        return df.withColumn("is_overlap", lit("none"))
    result = df.join(dau_overlap, on=["date", "identifier", "app_name"], how='left_outer').fillna("false", subset=["is_overlap"])
    return result

def date_deal_by_type(col_date, retention_type, is_report=False):
    import config
    if retention_type == config.RETENTION_TYPE_DAILY:
        return col_date[0:8]
    elif retention_type == config.RETENTION_TYPE_MONTHLY:
        return col_date[0:6] if not is_report else col_date[0:6] + '01'
    elif retention_type == config.RETENTION_TYPE_WEEKLY:
        day = datetime.strptime(col_date[0:8], '%Y%m%d')
        return (day + timedelta(days=-(day.weekday()))).strftime('%Y%m%d')
    else:
        return ''


def diff_days_lt(n, ns, rets, max_n=0):
    lt = 0
    meet_n = (max_n >= n)
    for idx, ni in enumerate(ns):
        if n >= ni:
            lt += rets[idx]
        if n == ni:
            meet_n = True
    return lt if meet_n else 0


# keep activate for n = 2, otherwise return n = 1 when retention type report has n = 1
def find_activate(ns, acts, retention_type):
    import config
    act = 0
    for ni, a in zip(ns, acts):
        if (ni != 1) or (1 in config.RETENTION_DICT[retention_type]):
            act = max(act, a)
    return act


VALID_PACKAGE_NAME = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$')


def get_af_package_name(app_id, bundle_id):
    pkg_name = app_id
    if not VALID_PACKAGE_NAME.match(app_id) and bundle_id and VALID_PACKAGE_NAME.match(bundle_id):
        pkg_name = bundle_id
    return pkg_name


def format_roi_summary_data_frame(spark, path, end_date, keys, udf_app_bundle=None):
    from pyspark.sql.functions import col, lit, when, row_number, substring

    df = spark.read.option("mergeSchema", "true").parquet(path)\
        .withColumn('act_date', substring(col('act_time'), 1, 8)).where(col('act_date') == end_date)
    if 'date' not in df.columns:
        df = df.withColumn('date', col('time'))
    df = df.withColumn('date', substring(col('date'), 1, 8))\
        .withColumn('app_name', when(col('act_app_name').isNotNull(), col('act_app_name')).otherwise(col('app_name'))).drop('act_app_name')\
        .drop('country').withColumnRenamed('act_country', 'country')\
        .withColumnRenamed('campaign_name', 'campaign')
    df = add_report_columns(df, keys)
    df = disabiguous_appsflyer_source(df, keys)
    if udf_app_bundle:
        df = df.withColumn('app_name', udf_app_bundle(col('app_name')))
    return df
