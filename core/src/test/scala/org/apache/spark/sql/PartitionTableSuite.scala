/*
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import com.pingcap.tikv.meta.TiDAGRequest
import org.apache.spark.sql.execution.{CoprocessorRDD, RegionTaskExec}

class PartitionTableSuite extends BaseTiSparkSuite {
  def enablePartitionForTiDB() = tidbStmt.execute("set @@tidb_enable_table_partition = 1")

  test("read from partition table stack overflow") {
    tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (mod(year(purchased), 1023)) (
                       |PARTITION p0 VALUES LESS THAN (1),
                       |PARTITION p2 VALUES LESS THAN (2),
                       |PARTITION p3 VALUES LESS THAN (3),
                       |PARTITION p4 VALUES LESS THAN (4),
                       |PARTITION p5 VALUES LESS THAN (5),
                       |PARTITION p6 VALUES LESS THAN (6),
                       |PARTITION p7 VALUES LESS THAN (7),
                       |PARTITION p8 VALUES LESS THAN (8),
                       |PARTITION p9 VALUES LESS THAN (9),
                       |PARTITION p10 VALUES LESS THAN (10),
                       |PARTITION p11 VALUES LESS THAN (11),
                       |PARTITION p12 VALUES LESS THAN (12),
                       |PARTITION p13 VALUES LESS THAN (13),
                       |PARTITION p14 VALUES LESS THAN (14),
                       |PARTITION p15 VALUES LESS THAN (15),
                       |PARTITION p16 VALUES LESS THAN (16),
                       |PARTITION p17 VALUES LESS THAN (17),
                       |PARTITION p18 VALUES LESS THAN (18),
                       |PARTITION p19 VALUES LESS THAN (19),
                       |PARTITION p20 VALUES LESS THAN (20),
                       |PARTITION p21 VALUES LESS THAN (21),
                       |PARTITION p22 VALUES LESS THAN (22),
                       |PARTITION p23 VALUES LESS THAN (23),
                       |PARTITION p24 VALUES LESS THAN (24),
                       |PARTITION p25 VALUES LESS THAN (25),
                       |PARTITION p26 VALUES LESS THAN (26),
                       |PARTITION p27 VALUES LESS THAN (27),
                       |PARTITION p28 VALUES LESS THAN (28),
                       |PARTITION p29 VALUES LESS THAN (29),
                       |PARTITION p30 VALUES LESS THAN (30),
                       |PARTITION p31 VALUES LESS THAN (31),
                       |PARTITION p32 VALUES LESS THAN (32),
                       |PARTITION p33 VALUES LESS THAN (33),
                       |PARTITION p34 VALUES LESS THAN (34),
                       |PARTITION p35 VALUES LESS THAN (35),
                       |PARTITION p36 VALUES LESS THAN (36),
                       |PARTITION p37 VALUES LESS THAN (37),
                       |PARTITION p38 VALUES LESS THAN (38),
                       |PARTITION p39 VALUES LESS THAN (39),
                       |PARTITION p40 VALUES LESS THAN (40),
                       |PARTITION p41 VALUES LESS THAN (41),
                       |PARTITION p42 VALUES LESS THAN (42),
                       |PARTITION p43 VALUES LESS THAN (43),
                       |PARTITION p44 VALUES LESS THAN (44),
                       |PARTITION p45 VALUES LESS THAN (45),
                       |PARTITION p46 VALUES LESS THAN (46),
                       |PARTITION p47 VALUES LESS THAN (47),
                       |PARTITION p48 VALUES LESS THAN (48),
                       |PARTITION p49 VALUES LESS THAN (49),
                       |PARTITION p50 VALUES LESS THAN (50),
                       |PARTITION p51 VALUES LESS THAN (51),
                       |PARTITION p52 VALUES LESS THAN (52),
                       |PARTITION p53 VALUES LESS THAN (53),
                       |PARTITION p54 VALUES LESS THAN (54),
                       |PARTITION p55 VALUES LESS THAN (55),
                       |PARTITION p56 VALUES LESS THAN (56),
                       |PARTITION p57 VALUES LESS THAN (57),
                       |PARTITION p58 VALUES LESS THAN (58),
                       |PARTITION p59 VALUES LESS THAN (59),
                       |PARTITION p60 VALUES LESS THAN (60),
                       |PARTITION p61 VALUES LESS THAN (61),
                       |PARTITION p62 VALUES LESS THAN (62),
                       |PARTITION p63 VALUES LESS THAN (63),
                       |PARTITION p64 VALUES LESS THAN (64),
                       |PARTITION p65 VALUES LESS THAN (65),
                       |PARTITION p66 VALUES LESS THAN (66),
                       |PARTITION p67 VALUES LESS THAN (67),
                       |PARTITION p68 VALUES LESS THAN (68),
                       |PARTITION p69 VALUES LESS THAN (69),
                       |PARTITION p70 VALUES LESS THAN (70),
                       |PARTITION p71 VALUES LESS THAN (71),
                       |PARTITION p72 VALUES LESS THAN (72),
                       |PARTITION p73 VALUES LESS THAN (73),
                       |PARTITION p74 VALUES LESS THAN (74),
                       |PARTITION p75 VALUES LESS THAN (75),
                       |PARTITION p76 VALUES LESS THAN (76),
                       |PARTITION p77 VALUES LESS THAN (77),
                       |PARTITION p78 VALUES LESS THAN (78),
                       |PARTITION p79 VALUES LESS THAN (79),
                       |PARTITION p80 VALUES LESS THAN (80),
                       |PARTITION p81 VALUES LESS THAN (81),
                       |PARTITION p82 VALUES LESS THAN (82),
                       |PARTITION p83 VALUES LESS THAN (83),
                       |PARTITION p84 VALUES LESS THAN (84),
                       |PARTITION p85 VALUES LESS THAN (85),
                       |PARTITION p86 VALUES LESS THAN (86),
                       |PARTITION p87 VALUES LESS THAN (87),
                       |PARTITION p88 VALUES LESS THAN (88),
                       |PARTITION p89 VALUES LESS THAN (89),
                       |PARTITION p90 VALUES LESS THAN (90),
                       |PARTITION p91 VALUES LESS THAN (91),
                       |PARTITION p92 VALUES LESS THAN (92),
                       |PARTITION p93 VALUES LESS THAN (93),
                       |PARTITION p94 VALUES LESS THAN (94),
                       |PARTITION p95 VALUES LESS THAN (95),
                       |PARTITION p96 VALUES LESS THAN (96),
                       |PARTITION p97 VALUES LESS THAN (97),
                       |PARTITION p98 VALUES LESS THAN (98),
                       |PARTITION p99 VALUES LESS THAN (99),
                       |PARTITION p100 VALUES LESS THAN (100),
                       |PARTITION p101 VALUES LESS THAN (101),
                       |PARTITION p102 VALUES LESS THAN (102),
                       |PARTITION p103 VALUES LESS THAN (103),
                       |PARTITION p104 VALUES LESS THAN (104),
                       |PARTITION p105 VALUES LESS THAN (105),
                       |PARTITION p106 VALUES LESS THAN (106),
                       |PARTITION p107 VALUES LESS THAN (107),
                       |PARTITION p108 VALUES LESS THAN (108),
                       |PARTITION p109 VALUES LESS THAN (109),
                       |PARTITION p110 VALUES LESS THAN (110),
                       |PARTITION p111 VALUES LESS THAN (111),
                       |PARTITION p112 VALUES LESS THAN (112),
                       |PARTITION p113 VALUES LESS THAN (113),
                       |PARTITION p114 VALUES LESS THAN (114),
                       |PARTITION p115 VALUES LESS THAN (115),
                       |PARTITION p116 VALUES LESS THAN (116),
                       |PARTITION p117 VALUES LESS THAN (117),
                       |PARTITION p118 VALUES LESS THAN (118),
                       |PARTITION p119 VALUES LESS THAN (119),
                       |PARTITION p120 VALUES LESS THAN (120),
                       |PARTITION p121 VALUES LESS THAN (121),
                       |PARTITION p122 VALUES LESS THAN (122),
                       |PARTITION p123 VALUES LESS THAN (123),
                       |PARTITION p124 VALUES LESS THAN (124),
                       |PARTITION p125 VALUES LESS THAN (125),
                       |PARTITION p126 VALUES LESS THAN (126),
                       |PARTITION p127 VALUES LESS THAN (127),
                       |PARTITION p128 VALUES LESS THAN (128),
                       |PARTITION p129 VALUES LESS THAN (129),
                       |PARTITION p130 VALUES LESS THAN (130),
                       |PARTITION p131 VALUES LESS THAN (131),
                       |PARTITION p132 VALUES LESS THAN (132),
                       |PARTITION p133 VALUES LESS THAN (133),
                       |PARTITION p134 VALUES LESS THAN (134),
                       |PARTITION p135 VALUES LESS THAN (135),
                       |PARTITION p136 VALUES LESS THAN (136),
                       |PARTITION p137 VALUES LESS THAN (137),
                       |PARTITION p138 VALUES LESS THAN (138),
                       |PARTITION p139 VALUES LESS THAN (139),
                       |PARTITION p140 VALUES LESS THAN (140),
                       |PARTITION p141 VALUES LESS THAN (141),
                       |PARTITION p142 VALUES LESS THAN (142),
                       |PARTITION p143 VALUES LESS THAN (143),
                       |PARTITION p144 VALUES LESS THAN (144),
                       |PARTITION p145 VALUES LESS THAN (145),
                       |PARTITION p146 VALUES LESS THAN (146),
                       |PARTITION p147 VALUES LESS THAN (147),
                       |PARTITION p148 VALUES LESS THAN (148),
                       |PARTITION p149 VALUES LESS THAN (149),
                       |PARTITION p150 VALUES LESS THAN (150),
                       |PARTITION p151 VALUES LESS THAN (151),
                       |PARTITION p152 VALUES LESS THAN (152),
                       |PARTITION p153 VALUES LESS THAN (153),
                       |PARTITION p154 VALUES LESS THAN (154),
                       |PARTITION p155 VALUES LESS THAN (155),
                       |PARTITION p156 VALUES LESS THAN (156),
                       |PARTITION p157 VALUES LESS THAN (157),
                       |PARTITION p158 VALUES LESS THAN (158),
                       |PARTITION p159 VALUES LESS THAN (159),
                       |PARTITION p160 VALUES LESS THAN (160),
                       |PARTITION p161 VALUES LESS THAN (161),
                       |PARTITION p162 VALUES LESS THAN (162),
                       |PARTITION p163 VALUES LESS THAN (163),
                       |PARTITION p164 VALUES LESS THAN (164),
                       |PARTITION p165 VALUES LESS THAN (165),
                       |PARTITION p166 VALUES LESS THAN (166),
                       |PARTITION p167 VALUES LESS THAN (167),
                       |PARTITION p168 VALUES LESS THAN (168),
                       |PARTITION p169 VALUES LESS THAN (169),
                       |PARTITION p170 VALUES LESS THAN (170),
                       |PARTITION p171 VALUES LESS THAN (171),
                       |PARTITION p172 VALUES LESS THAN (172),
                       |PARTITION p173 VALUES LESS THAN (173),
                       |PARTITION p174 VALUES LESS THAN (174),
                       |PARTITION p175 VALUES LESS THAN (175),
                       |PARTITION p176 VALUES LESS THAN (176),
                       |PARTITION p177 VALUES LESS THAN (177),
                       |PARTITION p178 VALUES LESS THAN (178),
                       |PARTITION p179 VALUES LESS THAN (179),
                       |PARTITION p180 VALUES LESS THAN (180),
                       |PARTITION p181 VALUES LESS THAN (181),
                       |PARTITION p182 VALUES LESS THAN (182),
                       |PARTITION p183 VALUES LESS THAN (183),
                       |PARTITION p184 VALUES LESS THAN (184),
                       |PARTITION p185 VALUES LESS THAN (185),
                       |PARTITION p186 VALUES LESS THAN (186),
                       |PARTITION p187 VALUES LESS THAN (187),
                       |PARTITION p188 VALUES LESS THAN (188),
                       |PARTITION p189 VALUES LESS THAN (189),
                       |PARTITION p190 VALUES LESS THAN (190),
                       |PARTITION p191 VALUES LESS THAN (191),
                       |PARTITION p192 VALUES LESS THAN (192),
                       |PARTITION p193 VALUES LESS THAN (193),
                       |PARTITION p194 VALUES LESS THAN (194),
                       |PARTITION p195 VALUES LESS THAN (195),
                       |PARTITION p196 VALUES LESS THAN (196),
                       |PARTITION p197 VALUES LESS THAN (197),
                       |PARTITION p198 VALUES LESS THAN (198),
                       |PARTITION p199 VALUES LESS THAN (199),
                       |PARTITION p200 VALUES LESS THAN (200),
                       |PARTITION p201 VALUES LESS THAN (201),
                       |PARTITION p202 VALUES LESS THAN (202),
                       |PARTITION p203 VALUES LESS THAN (203),
                       |PARTITION p204 VALUES LESS THAN (204),
                       |PARTITION p205 VALUES LESS THAN (205),
                       |PARTITION p206 VALUES LESS THAN (206),
                       |PARTITION p207 VALUES LESS THAN (207),
                       |PARTITION p208 VALUES LESS THAN (208),
                       |PARTITION p209 VALUES LESS THAN (209),
                       |PARTITION p210 VALUES LESS THAN (210),
                       |PARTITION p211 VALUES LESS THAN (211),
                       |PARTITION p212 VALUES LESS THAN (212),
                       |PARTITION p213 VALUES LESS THAN (213),
                       |PARTITION p214 VALUES LESS THAN (214),
                       |PARTITION p215 VALUES LESS THAN (215),
                       |PARTITION p216 VALUES LESS THAN (216),
                       |PARTITION p217 VALUES LESS THAN (217),
                       |PARTITION p218 VALUES LESS THAN (218),
                       |PARTITION p219 VALUES LESS THAN (219),
                       |PARTITION p220 VALUES LESS THAN (220),
                       |PARTITION p221 VALUES LESS THAN (221),
                       |PARTITION p222 VALUES LESS THAN (222),
                       |PARTITION p223 VALUES LESS THAN (223),
                       |PARTITION p224 VALUES LESS THAN (224),
                       |PARTITION p225 VALUES LESS THAN (225),
                       |PARTITION p226 VALUES LESS THAN (226),
                       |PARTITION p227 VALUES LESS THAN (227),
                       |PARTITION p228 VALUES LESS THAN (228),
                       |PARTITION p229 VALUES LESS THAN (229),
                       |PARTITION p230 VALUES LESS THAN (230),
                       |PARTITION p231 VALUES LESS THAN (231),
                       |PARTITION p232 VALUES LESS THAN (232),
                       |PARTITION p233 VALUES LESS THAN (233),
                       |PARTITION p234 VALUES LESS THAN (234),
                       |PARTITION p235 VALUES LESS THAN (235),
                       |PARTITION p236 VALUES LESS THAN (236),
                       |PARTITION p237 VALUES LESS THAN (237),
                       |PARTITION p238 VALUES LESS THAN (238),
                       |PARTITION p239 VALUES LESS THAN (239),
                       |PARTITION p240 VALUES LESS THAN (240),
                       |PARTITION p241 VALUES LESS THAN (241),
                       |PARTITION p242 VALUES LESS THAN (242),
                       |PARTITION p243 VALUES LESS THAN (243),
                       |PARTITION p244 VALUES LESS THAN (244),
                       |PARTITION p245 VALUES LESS THAN (245),
                       |PARTITION p246 VALUES LESS THAN (246),
                       |PARTITION p247 VALUES LESS THAN (247),
                       |PARTITION p248 VALUES LESS THAN (248),
                       |PARTITION p249 VALUES LESS THAN (249),
                       |PARTITION p250 VALUES LESS THAN (250),
                       |PARTITION p251 VALUES LESS THAN (251),
                       |PARTITION p252 VALUES LESS THAN (252),
                       |PARTITION p253 VALUES LESS THAN (253),
                       |PARTITION p254 VALUES LESS THAN (254),
                       |PARTITION p255 VALUES LESS THAN (255),
                       |PARTITION p256 VALUES LESS THAN (256),
                       |PARTITION p257 VALUES LESS THAN (257),
                       |PARTITION p258 VALUES LESS THAN (258),
                       |PARTITION p259 VALUES LESS THAN (259),
                       |PARTITION p260 VALUES LESS THAN (260),
                       |PARTITION p261 VALUES LESS THAN (261),
                       |PARTITION p262 VALUES LESS THAN (262),
                       |PARTITION p263 VALUES LESS THAN (263),
                       |PARTITION p264 VALUES LESS THAN (264),
                       |PARTITION p265 VALUES LESS THAN (265),
                       |PARTITION p266 VALUES LESS THAN (266),
                       |PARTITION p267 VALUES LESS THAN (267),
                       |PARTITION p268 VALUES LESS THAN (268),
                       |PARTITION p269 VALUES LESS THAN (269),
                       |PARTITION p270 VALUES LESS THAN (270),
                       |PARTITION p271 VALUES LESS THAN (271),
                       |PARTITION p272 VALUES LESS THAN (272),
                       |PARTITION p273 VALUES LESS THAN (273),
                       |PARTITION p274 VALUES LESS THAN (274),
                       |PARTITION p275 VALUES LESS THAN (275),
                       |PARTITION p276 VALUES LESS THAN (276),
                       |PARTITION p277 VALUES LESS THAN (277),
                       |PARTITION p278 VALUES LESS THAN (278),
                       |PARTITION p279 VALUES LESS THAN (279),
                       |PARTITION p280 VALUES LESS THAN (280),
                       |PARTITION p281 VALUES LESS THAN (281),
                       |PARTITION p282 VALUES LESS THAN (282),
                       |PARTITION p283 VALUES LESS THAN (283),
                       |PARTITION p284 VALUES LESS THAN (284),
                       |PARTITION p285 VALUES LESS THAN (285),
                       |PARTITION p286 VALUES LESS THAN (286),
                       |PARTITION p287 VALUES LESS THAN (287),
                       |PARTITION p288 VALUES LESS THAN (288),
                       |PARTITION p289 VALUES LESS THAN (289),
                       |PARTITION p290 VALUES LESS THAN (290),
                       |PARTITION p291 VALUES LESS THAN (291),
                       |PARTITION p292 VALUES LESS THAN (292),
                       |PARTITION p293 VALUES LESS THAN (293),
                       |PARTITION p294 VALUES LESS THAN (294),
                       |PARTITION p295 VALUES LESS THAN (295),
                       |PARTITION p296 VALUES LESS THAN (296),
                       |PARTITION p297 VALUES LESS THAN (297),
                       |PARTITION p298 VALUES LESS THAN (298),
                       |PARTITION p299 VALUES LESS THAN (299),
                       |PARTITION p300 VALUES LESS THAN (300),
                       |PARTITION p301 VALUES LESS THAN (301),
                       |PARTITION p302 VALUES LESS THAN (302),
                       |PARTITION p303 VALUES LESS THAN (303),
                       |PARTITION p304 VALUES LESS THAN (304),
                       |PARTITION p305 VALUES LESS THAN (305),
                       |PARTITION p306 VALUES LESS THAN (306),
                       |PARTITION p307 VALUES LESS THAN (307),
                       |PARTITION p308 VALUES LESS THAN (308),
                       |PARTITION p309 VALUES LESS THAN (309),
                       |PARTITION p310 VALUES LESS THAN (310),
                       |PARTITION p311 VALUES LESS THAN (311),
                       |PARTITION p312 VALUES LESS THAN (312),
                       |PARTITION p313 VALUES LESS THAN (313),
                       |PARTITION p314 VALUES LESS THAN (314),
                       |PARTITION p315 VALUES LESS THAN (315),
                       |PARTITION p316 VALUES LESS THAN (316),
                       |PARTITION p317 VALUES LESS THAN (317),
                       |PARTITION p318 VALUES LESS THAN (318),
                       |PARTITION p319 VALUES LESS THAN (319),
                       |PARTITION p320 VALUES LESS THAN (320),
                       |PARTITION p321 VALUES LESS THAN (321),
                       |PARTITION p322 VALUES LESS THAN (322),
                       |PARTITION p323 VALUES LESS THAN (323),
                       |PARTITION p324 VALUES LESS THAN (324),
                       |PARTITION p325 VALUES LESS THAN (325),
                       |PARTITION p326 VALUES LESS THAN (326),
                       |PARTITION p327 VALUES LESS THAN (327),
                       |PARTITION p328 VALUES LESS THAN (328),
                       |PARTITION p329 VALUES LESS THAN (329),
                       |PARTITION p330 VALUES LESS THAN (330),
                       |PARTITION p331 VALUES LESS THAN (331),
                       |PARTITION p332 VALUES LESS THAN (332),
                       |PARTITION p333 VALUES LESS THAN (333),
                       |PARTITION p334 VALUES LESS THAN (334),
                       |PARTITION p335 VALUES LESS THAN (335),
                       |PARTITION p336 VALUES LESS THAN (336),
                       |PARTITION p337 VALUES LESS THAN (337),
                       |PARTITION p338 VALUES LESS THAN (338),
                       |PARTITION p339 VALUES LESS THAN (339),
                       |PARTITION p340 VALUES LESS THAN (340),
                       |PARTITION p341 VALUES LESS THAN (341),
                       |PARTITION p342 VALUES LESS THAN (342),
                       |PARTITION p343 VALUES LESS THAN (343),
                       |PARTITION p344 VALUES LESS THAN (344),
                       |PARTITION p345 VALUES LESS THAN (345),
                       |PARTITION p346 VALUES LESS THAN (346),
                       |PARTITION p347 VALUES LESS THAN (347),
                       |PARTITION p348 VALUES LESS THAN (348),
                       |PARTITION p349 VALUES LESS THAN (349),
                       |PARTITION p350 VALUES LESS THAN (350),
                       |PARTITION p351 VALUES LESS THAN (351),
                       |PARTITION p352 VALUES LESS THAN (352),
                       |PARTITION p353 VALUES LESS THAN (353),
                       |PARTITION p354 VALUES LESS THAN (354),
                       |PARTITION p355 VALUES LESS THAN (355),
                       |PARTITION p356 VALUES LESS THAN (356),
                       |PARTITION p357 VALUES LESS THAN (357),
                       |PARTITION p358 VALUES LESS THAN (358),
                       |PARTITION p359 VALUES LESS THAN (359),
                       |PARTITION p360 VALUES LESS THAN (360),
                       |PARTITION p361 VALUES LESS THAN (361),
                       |PARTITION p362 VALUES LESS THAN (362),
                       |PARTITION p363 VALUES LESS THAN (363),
                       |PARTITION p364 VALUES LESS THAN (364),
                       |PARTITION p365 VALUES LESS THAN (365),
                       |PARTITION p366 VALUES LESS THAN (366),
                       |PARTITION p367 VALUES LESS THAN (367),
                       |PARTITION p368 VALUES LESS THAN (368),
                       |PARTITION p369 VALUES LESS THAN (369),
                       |PARTITION p370 VALUES LESS THAN (370),
                       |PARTITION p371 VALUES LESS THAN (371),
                       |PARTITION p372 VALUES LESS THAN (372),
                       |PARTITION p373 VALUES LESS THAN (373),
                       |PARTITION p374 VALUES LESS THAN (374),
                       |PARTITION p375 VALUES LESS THAN (375),
                       |PARTITION p376 VALUES LESS THAN (376),
                       |PARTITION p377 VALUES LESS THAN (377),
                       |PARTITION p378 VALUES LESS THAN (378),
                       |PARTITION p379 VALUES LESS THAN (379),
                       |PARTITION p380 VALUES LESS THAN (380),
                       |PARTITION p381 VALUES LESS THAN (381),
                       |PARTITION p382 VALUES LESS THAN (382),
                       |PARTITION p383 VALUES LESS THAN (383),
                       |PARTITION p384 VALUES LESS THAN (384),
                       |PARTITION p385 VALUES LESS THAN (385),
                       |PARTITION p386 VALUES LESS THAN (386),
                       |PARTITION p387 VALUES LESS THAN (387),
                       |PARTITION p388 VALUES LESS THAN (388),
                       |PARTITION p389 VALUES LESS THAN (389),
                       |PARTITION p390 VALUES LESS THAN (390),
                       |PARTITION p391 VALUES LESS THAN (391),
                       |PARTITION p392 VALUES LESS THAN (392),
                       |PARTITION p393 VALUES LESS THAN (393),
                       |PARTITION p394 VALUES LESS THAN (394),
                       |PARTITION p395 VALUES LESS THAN (395),
                       |PARTITION p396 VALUES LESS THAN (396),
                       |PARTITION p397 VALUES LESS THAN (397),
                       |PARTITION p398 VALUES LESS THAN (398),
                       |PARTITION p399 VALUES LESS THAN (399),
                       |PARTITION p400 VALUES LESS THAN (400),
                       |PARTITION p401 VALUES LESS THAN (401),
                       |PARTITION p402 VALUES LESS THAN (402),
                       |PARTITION p403 VALUES LESS THAN (403),
                       |PARTITION p404 VALUES LESS THAN (404),
                       |PARTITION p405 VALUES LESS THAN (405),
                       |PARTITION p406 VALUES LESS THAN (406),
                       |PARTITION p407 VALUES LESS THAN (407),
                       |PARTITION p408 VALUES LESS THAN (408),
                       |PARTITION p409 VALUES LESS THAN (409),
                       |PARTITION p410 VALUES LESS THAN (410),
                       |PARTITION p411 VALUES LESS THAN (411),
                       |PARTITION p412 VALUES LESS THAN (412),
                       |PARTITION p413 VALUES LESS THAN (413),
                       |PARTITION p414 VALUES LESS THAN (414),
                       |PARTITION p415 VALUES LESS THAN (415),
                       |PARTITION p416 VALUES LESS THAN (416),
                       |PARTITION p417 VALUES LESS THAN (417),
                       |PARTITION p418 VALUES LESS THAN (418),
                       |PARTITION p419 VALUES LESS THAN (419),
                       |PARTITION p420 VALUES LESS THAN (420),
                       |PARTITION p421 VALUES LESS THAN (421),
                       |PARTITION p422 VALUES LESS THAN (422),
                       |PARTITION p423 VALUES LESS THAN (423),
                       |PARTITION p424 VALUES LESS THAN (424),
                       |PARTITION p425 VALUES LESS THAN (425),
                       |PARTITION p426 VALUES LESS THAN (426),
                       |PARTITION p427 VALUES LESS THAN (427),
                       |PARTITION p428 VALUES LESS THAN (428),
                       |PARTITION p429 VALUES LESS THAN (429),
                       |PARTITION p430 VALUES LESS THAN (430),
                       |PARTITION p431 VALUES LESS THAN (431),
                       |PARTITION p432 VALUES LESS THAN (432),
                       |PARTITION p433 VALUES LESS THAN (433),
                       |PARTITION p434 VALUES LESS THAN (434),
                       |PARTITION p435 VALUES LESS THAN (435),
                       |PARTITION p436 VALUES LESS THAN (436),
                       |PARTITION p437 VALUES LESS THAN (437),
                       |PARTITION p438 VALUES LESS THAN (438),
                       |PARTITION p439 VALUES LESS THAN (439),
                       |PARTITION p440 VALUES LESS THAN (440),
                       |PARTITION p441 VALUES LESS THAN (441),
                       |PARTITION p442 VALUES LESS THAN (442),
                       |PARTITION p443 VALUES LESS THAN (443),
                       |PARTITION p444 VALUES LESS THAN (444),
                       |PARTITION p445 VALUES LESS THAN (445),
                       |PARTITION p446 VALUES LESS THAN (446),
                       |PARTITION p447 VALUES LESS THAN (447),
                       |PARTITION p448 VALUES LESS THAN (448),
                       |PARTITION p449 VALUES LESS THAN (449),
                       |PARTITION p450 VALUES LESS THAN (450),
                       |PARTITION p451 VALUES LESS THAN (451),
                       |PARTITION p452 VALUES LESS THAN (452),
                       |PARTITION p453 VALUES LESS THAN (453),
                       |PARTITION p454 VALUES LESS THAN (454),
                       |PARTITION p455 VALUES LESS THAN (455),
                       |PARTITION p456 VALUES LESS THAN (456),
                       |PARTITION p457 VALUES LESS THAN (457),
                       |PARTITION p458 VALUES LESS THAN (458),
                       |PARTITION p459 VALUES LESS THAN (459),
                       |PARTITION p460 VALUES LESS THAN (460),
                       |PARTITION p461 VALUES LESS THAN (461),
                       |PARTITION p462 VALUES LESS THAN (462),
                       |PARTITION p463 VALUES LESS THAN (463),
                       |PARTITION p464 VALUES LESS THAN (464),
                       |PARTITION p465 VALUES LESS THAN (465),
                       |PARTITION p466 VALUES LESS THAN (466),
                       |PARTITION p467 VALUES LESS THAN (467),
                       |PARTITION p468 VALUES LESS THAN (468),
                       |PARTITION p469 VALUES LESS THAN (469),
                       |PARTITION p470 VALUES LESS THAN (470),
                       |PARTITION p471 VALUES LESS THAN (471),
                       |PARTITION p472 VALUES LESS THAN (472),
                       |PARTITION p473 VALUES LESS THAN (473),
                       |PARTITION p474 VALUES LESS THAN (474),
                       |PARTITION p475 VALUES LESS THAN (475),
                       |PARTITION p476 VALUES LESS THAN (476),
                       |PARTITION p477 VALUES LESS THAN (477),
                       |PARTITION p478 VALUES LESS THAN (478),
                       |PARTITION p479 VALUES LESS THAN (479),
                       |PARTITION p480 VALUES LESS THAN (480),
                       |PARTITION p481 VALUES LESS THAN (481),
                       |PARTITION p482 VALUES LESS THAN (482),
                       |PARTITION p483 VALUES LESS THAN (483),
                       |PARTITION p484 VALUES LESS THAN (484),
                       |PARTITION p485 VALUES LESS THAN (485),
                       |PARTITION p486 VALUES LESS THAN (486),
                       |PARTITION p487 VALUES LESS THAN (487),
                       |PARTITION p488 VALUES LESS THAN (488),
                       |PARTITION p489 VALUES LESS THAN (489),
                       |PARTITION p490 VALUES LESS THAN (490),
                       |PARTITION p491 VALUES LESS THAN (491),
                       |PARTITION p492 VALUES LESS THAN (492),
                       |PARTITION p493 VALUES LESS THAN (493),
                       |PARTITION p494 VALUES LESS THAN (494),
                       |PARTITION p495 VALUES LESS THAN (495),
                       |PARTITION p496 VALUES LESS THAN (496),
                       |PARTITION p497 VALUES LESS THAN (497),
                       |PARTITION p498 VALUES LESS THAN (498),
                       |PARTITION p499 VALUES LESS THAN (499),
                       |PARTITION p500 VALUES LESS THAN (500),
                       |PARTITION p501 VALUES LESS THAN (501),
                       |PARTITION p502 VALUES LESS THAN (502),
                       |PARTITION p503 VALUES LESS THAN (503),
                       |PARTITION p504 VALUES LESS THAN (504),
                       |PARTITION p505 VALUES LESS THAN (505),
                       |PARTITION p506 VALUES LESS THAN (506),
                       |PARTITION p507 VALUES LESS THAN (507),
                       |PARTITION p508 VALUES LESS THAN (508),
                       |PARTITION p509 VALUES LESS THAN (509),
                       |PARTITION p510 VALUES LESS THAN (510),
                       |PARTITION p511 VALUES LESS THAN (511),
                       |PARTITION p512 VALUES LESS THAN (512),
                       |PARTITION p513 VALUES LESS THAN (513),
                       |PARTITION p514 VALUES LESS THAN (514),
                       |PARTITION p515 VALUES LESS THAN (515),
                       |PARTITION p516 VALUES LESS THAN (516),
                       |PARTITION p517 VALUES LESS THAN (517),
                       |PARTITION p518 VALUES LESS THAN (518),
                       |PARTITION p519 VALUES LESS THAN (519),
                       |PARTITION p520 VALUES LESS THAN (520),
                       |PARTITION p521 VALUES LESS THAN (521),
                       |PARTITION p522 VALUES LESS THAN (522),
                       |PARTITION p523 VALUES LESS THAN (523),
                       |PARTITION p524 VALUES LESS THAN (524),
                       |PARTITION p525 VALUES LESS THAN (525),
                       |PARTITION p526 VALUES LESS THAN (526),
                       |PARTITION p527 VALUES LESS THAN (527),
                       |PARTITION p528 VALUES LESS THAN (528),
                       |PARTITION p529 VALUES LESS THAN (529),
                       |PARTITION p530 VALUES LESS THAN (530),
                       |PARTITION p531 VALUES LESS THAN (531),
                       |PARTITION p532 VALUES LESS THAN (532),
                       |PARTITION p533 VALUES LESS THAN (533),
                       |PARTITION p534 VALUES LESS THAN (534),
                       |PARTITION p535 VALUES LESS THAN (535),
                       |PARTITION p536 VALUES LESS THAN (536),
                       |PARTITION p537 VALUES LESS THAN (537),
                       |PARTITION p538 VALUES LESS THAN (538),
                       |PARTITION p539 VALUES LESS THAN (539),
                       |PARTITION p540 VALUES LESS THAN (540),
                       |PARTITION p541 VALUES LESS THAN (541),
                       |PARTITION p542 VALUES LESS THAN (542),
                       |PARTITION p543 VALUES LESS THAN (543),
                       |PARTITION p544 VALUES LESS THAN (544),
                       |PARTITION p545 VALUES LESS THAN (545),
                       |PARTITION p546 VALUES LESS THAN (546),
                       |PARTITION p547 VALUES LESS THAN (547),
                       |PARTITION p548 VALUES LESS THAN (548),
                       |PARTITION p549 VALUES LESS THAN (549),
                       |PARTITION p550 VALUES LESS THAN (550),
                       |PARTITION p551 VALUES LESS THAN (551),
                       |PARTITION p552 VALUES LESS THAN (552),
                       |PARTITION p553 VALUES LESS THAN (553),
                       |PARTITION p554 VALUES LESS THAN (554),
                       |PARTITION p555 VALUES LESS THAN (555),
                       |PARTITION p556 VALUES LESS THAN (556),
                       |PARTITION p557 VALUES LESS THAN (557),
                       |PARTITION p558 VALUES LESS THAN (558),
                       |PARTITION p559 VALUES LESS THAN (559),
                       |PARTITION p560 VALUES LESS THAN (560),
                       |PARTITION p561 VALUES LESS THAN (561),
                       |PARTITION p562 VALUES LESS THAN (562),
                       |PARTITION p563 VALUES LESS THAN (563),
                       |PARTITION p564 VALUES LESS THAN (564),
                       |PARTITION p565 VALUES LESS THAN (565),
                       |PARTITION p566 VALUES LESS THAN (566),
                       |PARTITION p567 VALUES LESS THAN (567),
                       |PARTITION p568 VALUES LESS THAN (568),
                       |PARTITION p569 VALUES LESS THAN (569),
                       |PARTITION p570 VALUES LESS THAN (570),
                       |PARTITION p571 VALUES LESS THAN (571),
                       |PARTITION p572 VALUES LESS THAN (572),
                       |PARTITION p573 VALUES LESS THAN (573),
                       |PARTITION p574 VALUES LESS THAN (574),
                       |PARTITION p575 VALUES LESS THAN (575),
                       |PARTITION p576 VALUES LESS THAN (576),
                       |PARTITION p577 VALUES LESS THAN (577),
                       |PARTITION p578 VALUES LESS THAN (578),
                       |PARTITION p579 VALUES LESS THAN (579),
                       |PARTITION p580 VALUES LESS THAN (580),
                       |PARTITION p581 VALUES LESS THAN (581),
                       |PARTITION p582 VALUES LESS THAN (582),
                       |PARTITION p583 VALUES LESS THAN (583),
                       |PARTITION p584 VALUES LESS THAN (584),
                       |PARTITION p585 VALUES LESS THAN (585),
                       |PARTITION p586 VALUES LESS THAN (586),
                       |PARTITION p587 VALUES LESS THAN (587),
                       |PARTITION p588 VALUES LESS THAN (588),
                       |PARTITION p589 VALUES LESS THAN (589),
                       |PARTITION p590 VALUES LESS THAN (590),
                       |PARTITION p591 VALUES LESS THAN (591),
                       |PARTITION p592 VALUES LESS THAN (592),
                       |PARTITION p593 VALUES LESS THAN (593),
                       |PARTITION p594 VALUES LESS THAN (594),
                       |PARTITION p595 VALUES LESS THAN (595),
                       |PARTITION p596 VALUES LESS THAN (596),
                       |PARTITION p597 VALUES LESS THAN (597),
                       |PARTITION p598 VALUES LESS THAN (598),
                       |PARTITION p599 VALUES LESS THAN (599),
                       |PARTITION p600 VALUES LESS THAN (600),
                       |PARTITION p601 VALUES LESS THAN (601),
                       |PARTITION p602 VALUES LESS THAN (602),
                       |PARTITION p603 VALUES LESS THAN (603),
                       |PARTITION p604 VALUES LESS THAN (604),
                       |PARTITION p605 VALUES LESS THAN (605),
                       |PARTITION p606 VALUES LESS THAN (606),
                       |PARTITION p607 VALUES LESS THAN (607),
                       |PARTITION p608 VALUES LESS THAN (608),
                       |PARTITION p609 VALUES LESS THAN (609),
                       |PARTITION p610 VALUES LESS THAN (610),
                       |PARTITION p611 VALUES LESS THAN (611),
                       |PARTITION p612 VALUES LESS THAN (612),
                       |PARTITION p613 VALUES LESS THAN (613),
                       |PARTITION p614 VALUES LESS THAN (614),
                       |PARTITION p615 VALUES LESS THAN (615),
                       |PARTITION p616 VALUES LESS THAN (616),
                       |PARTITION p617 VALUES LESS THAN (617),
                       |PARTITION p618 VALUES LESS THAN (618),
                       |PARTITION p619 VALUES LESS THAN (619),
                       |PARTITION p620 VALUES LESS THAN (620),
                       |PARTITION p621 VALUES LESS THAN (621),
                       |PARTITION p622 VALUES LESS THAN (622),
                       |PARTITION p623 VALUES LESS THAN (623),
                       |PARTITION p624 VALUES LESS THAN (624),
                       |PARTITION p625 VALUES LESS THAN (625),
                       |PARTITION p626 VALUES LESS THAN (626),
                       |PARTITION p627 VALUES LESS THAN (627),
                       |PARTITION p628 VALUES LESS THAN (628),
                       |PARTITION p629 VALUES LESS THAN (629),
                       |PARTITION p630 VALUES LESS THAN (630),
                       |PARTITION p631 VALUES LESS THAN (631),
                       |PARTITION p632 VALUES LESS THAN (632),
                       |PARTITION p633 VALUES LESS THAN (633),
                       |PARTITION p634 VALUES LESS THAN (634),
                       |PARTITION p635 VALUES LESS THAN (635),
                       |PARTITION p636 VALUES LESS THAN (636),
                       |PARTITION p637 VALUES LESS THAN (637),
                       |PARTITION p638 VALUES LESS THAN (638),
                       |PARTITION p639 VALUES LESS THAN (639),
                       |PARTITION p640 VALUES LESS THAN (640),
                       |PARTITION p641 VALUES LESS THAN (641),
                       |PARTITION p642 VALUES LESS THAN (642),
                       |PARTITION p643 VALUES LESS THAN (643),
                       |PARTITION p644 VALUES LESS THAN (644),
                       |PARTITION p645 VALUES LESS THAN (645),
                       |PARTITION p646 VALUES LESS THAN (646),
                       |PARTITION p647 VALUES LESS THAN (647),
                       |PARTITION p648 VALUES LESS THAN (648),
                       |PARTITION p649 VALUES LESS THAN (649),
                       |PARTITION p650 VALUES LESS THAN (650),
                       |PARTITION p651 VALUES LESS THAN (651),
                       |PARTITION p652 VALUES LESS THAN (652),
                       |PARTITION p653 VALUES LESS THAN (653),
                       |PARTITION p654 VALUES LESS THAN (654),
                       |PARTITION p655 VALUES LESS THAN (655),
                       |PARTITION p656 VALUES LESS THAN (656),
                       |PARTITION p657 VALUES LESS THAN (657),
                       |PARTITION p658 VALUES LESS THAN (658),
                       |PARTITION p659 VALUES LESS THAN (659),
                       |PARTITION p660 VALUES LESS THAN (660),
                       |PARTITION p661 VALUES LESS THAN (661),
                       |PARTITION p662 VALUES LESS THAN (662),
                       |PARTITION p663 VALUES LESS THAN (663),
                       |PARTITION p664 VALUES LESS THAN (664),
                       |PARTITION p665 VALUES LESS THAN (665),
                       |PARTITION p666 VALUES LESS THAN (666),
                       |PARTITION p667 VALUES LESS THAN (667),
                       |PARTITION p668 VALUES LESS THAN (668),
                       |PARTITION p669 VALUES LESS THAN (669),
                       |PARTITION p670 VALUES LESS THAN (670),
                       |PARTITION p671 VALUES LESS THAN (671),
                       |PARTITION p672 VALUES LESS THAN (672),
                       |PARTITION p673 VALUES LESS THAN (673),
                       |PARTITION p674 VALUES LESS THAN (674),
                       |PARTITION p675 VALUES LESS THAN (675),
                       |PARTITION p676 VALUES LESS THAN (676),
                       |PARTITION p677 VALUES LESS THAN (677),
                       |PARTITION p678 VALUES LESS THAN (678),
                       |PARTITION p679 VALUES LESS THAN (679),
                       |PARTITION p680 VALUES LESS THAN (680),
                       |PARTITION p681 VALUES LESS THAN (681),
                       |PARTITION p682 VALUES LESS THAN (682),
                       |PARTITION p683 VALUES LESS THAN (683),
                       |PARTITION p684 VALUES LESS THAN (684),
                       |PARTITION p685 VALUES LESS THAN (685),
                       |PARTITION p686 VALUES LESS THAN (686),
                       |PARTITION p687 VALUES LESS THAN (687),
                       |PARTITION p688 VALUES LESS THAN (688),
                       |PARTITION p689 VALUES LESS THAN (689),
                       |PARTITION p690 VALUES LESS THAN (690),
                       |PARTITION p691 VALUES LESS THAN (691),
                       |PARTITION p692 VALUES LESS THAN (692),
                       |PARTITION p693 VALUES LESS THAN (693),
                       |PARTITION p694 VALUES LESS THAN (694),
                       |PARTITION p695 VALUES LESS THAN (695),
                       |PARTITION p696 VALUES LESS THAN (696),
                       |PARTITION p697 VALUES LESS THAN (697),
                       |PARTITION p698 VALUES LESS THAN (698),
                       |PARTITION p699 VALUES LESS THAN (699),
                       |PARTITION p700 VALUES LESS THAN (700),
                       |PARTITION p701 VALUES LESS THAN (701),
                       |PARTITION p702 VALUES LESS THAN (702),
                       |PARTITION p703 VALUES LESS THAN (703),
                       |PARTITION p704 VALUES LESS THAN (704),
                       |PARTITION p705 VALUES LESS THAN (705),
                       |PARTITION p706 VALUES LESS THAN (706),
                       |PARTITION p707 VALUES LESS THAN (707),
                       |PARTITION p708 VALUES LESS THAN (708),
                       |PARTITION p709 VALUES LESS THAN (709),
                       |PARTITION p710 VALUES LESS THAN (710),
                       |PARTITION p711 VALUES LESS THAN (711),
                       |PARTITION p712 VALUES LESS THAN (712),
                       |PARTITION p713 VALUES LESS THAN (713),
                       |PARTITION p714 VALUES LESS THAN (714),
                       |PARTITION p715 VALUES LESS THAN (715),
                       |PARTITION p716 VALUES LESS THAN (716),
                       |PARTITION p717 VALUES LESS THAN (717),
                       |PARTITION p718 VALUES LESS THAN (718),
                       |PARTITION p719 VALUES LESS THAN (719),
                       |PARTITION p720 VALUES LESS THAN (720),
                       |PARTITION p721 VALUES LESS THAN (721),
                       |PARTITION p722 VALUES LESS THAN (722),
                       |PARTITION p723 VALUES LESS THAN (723),
                       |PARTITION p724 VALUES LESS THAN (724),
                       |PARTITION p725 VALUES LESS THAN (725),
                       |PARTITION p726 VALUES LESS THAN (726),
                       |PARTITION p727 VALUES LESS THAN (727),
                       |PARTITION p728 VALUES LESS THAN (728),
                       |PARTITION p729 VALUES LESS THAN (729),
                       |PARTITION p730 VALUES LESS THAN (730),
                       |PARTITION p731 VALUES LESS THAN (731),
                       |PARTITION p732 VALUES LESS THAN (732),
                       |PARTITION p733 VALUES LESS THAN (733),
                       |PARTITION p734 VALUES LESS THAN (734),
                       |PARTITION p735 VALUES LESS THAN (735),
                       |PARTITION p736 VALUES LESS THAN (736),
                       |PARTITION p737 VALUES LESS THAN (737),
                       |PARTITION p738 VALUES LESS THAN (738),
                       |PARTITION p739 VALUES LESS THAN (739),
                       |PARTITION p740 VALUES LESS THAN (740),
                       |PARTITION p741 VALUES LESS THAN (741),
                       |PARTITION p742 VALUES LESS THAN (742),
                       |PARTITION p743 VALUES LESS THAN (743),
                       |PARTITION p744 VALUES LESS THAN (744),
                       |PARTITION p745 VALUES LESS THAN (745),
                       |PARTITION p746 VALUES LESS THAN (746),
                       |PARTITION p747 VALUES LESS THAN (747),
                       |PARTITION p748 VALUES LESS THAN (748),
                       |PARTITION p749 VALUES LESS THAN (749),
                       |PARTITION p750 VALUES LESS THAN (750),
                       |PARTITION p751 VALUES LESS THAN (751),
                       |PARTITION p752 VALUES LESS THAN (752),
                       |PARTITION p753 VALUES LESS THAN (753),
                       |PARTITION p754 VALUES LESS THAN (754),
                       |PARTITION p755 VALUES LESS THAN (755),
                       |PARTITION p756 VALUES LESS THAN (756),
                       |PARTITION p757 VALUES LESS THAN (757),
                       |PARTITION p758 VALUES LESS THAN (758),
                       |PARTITION p759 VALUES LESS THAN (759),
                       |PARTITION p760 VALUES LESS THAN (760),
                       |PARTITION p761 VALUES LESS THAN (761),
                       |PARTITION p762 VALUES LESS THAN (762),
                       |PARTITION p763 VALUES LESS THAN (763),
                       |PARTITION p764 VALUES LESS THAN (764),
                       |PARTITION p765 VALUES LESS THAN (765),
                       |PARTITION p766 VALUES LESS THAN (766),
                       |PARTITION p767 VALUES LESS THAN (767),
                       |PARTITION p768 VALUES LESS THAN (768),
                       |PARTITION p769 VALUES LESS THAN (769),
                       |PARTITION p770 VALUES LESS THAN (770),
                       |PARTITION p771 VALUES LESS THAN (771),
                       |PARTITION p772 VALUES LESS THAN (772),
                       |PARTITION p773 VALUES LESS THAN (773),
                       |PARTITION p774 VALUES LESS THAN (774),
                       |PARTITION p775 VALUES LESS THAN (775),
                       |PARTITION p776 VALUES LESS THAN (776),
                       |PARTITION p777 VALUES LESS THAN (777),
                       |PARTITION p778 VALUES LESS THAN (778),
                       |PARTITION p779 VALUES LESS THAN (779),
                       |PARTITION p780 VALUES LESS THAN (780),
                       |PARTITION p781 VALUES LESS THAN (781),
                       |PARTITION p782 VALUES LESS THAN (782),
                       |PARTITION p783 VALUES LESS THAN (783),
                       |PARTITION p784 VALUES LESS THAN (784),
                       |PARTITION p785 VALUES LESS THAN (785),
                       |PARTITION p786 VALUES LESS THAN (786),
                       |PARTITION p787 VALUES LESS THAN (787),
                       |PARTITION p788 VALUES LESS THAN (788),
                       |PARTITION p789 VALUES LESS THAN (789),
                       |PARTITION p790 VALUES LESS THAN (790),
                       |PARTITION p791 VALUES LESS THAN (791),
                       |PARTITION p792 VALUES LESS THAN (792),
                       |PARTITION p793 VALUES LESS THAN (793),
                       |PARTITION p794 VALUES LESS THAN (794),
                       |PARTITION p795 VALUES LESS THAN (795),
                       |PARTITION p796 VALUES LESS THAN (796),
                       |PARTITION p797 VALUES LESS THAN (797),
                       |PARTITION p798 VALUES LESS THAN (798),
                       |PARTITION p799 VALUES LESS THAN (799),
                       |PARTITION p800 VALUES LESS THAN (800),
                       |PARTITION p801 VALUES LESS THAN (801),
                       |PARTITION p802 VALUES LESS THAN (802),
                       |PARTITION p803 VALUES LESS THAN (803),
                       |PARTITION p804 VALUES LESS THAN (804),
                       |PARTITION p805 VALUES LESS THAN (805),
                       |PARTITION p806 VALUES LESS THAN (806),
                       |PARTITION p807 VALUES LESS THAN (807),
                       |PARTITION p808 VALUES LESS THAN (808),
                       |PARTITION p809 VALUES LESS THAN (809),
                       |PARTITION p810 VALUES LESS THAN (810),
                       |PARTITION p811 VALUES LESS THAN (811),
                       |PARTITION p812 VALUES LESS THAN (812),
                       |PARTITION p813 VALUES LESS THAN (813),
                       |PARTITION p814 VALUES LESS THAN (814),
                       |PARTITION p815 VALUES LESS THAN (815),
                       |PARTITION p816 VALUES LESS THAN (816),
                       |PARTITION p817 VALUES LESS THAN (817),
                       |PARTITION p818 VALUES LESS THAN (818),
                       |PARTITION p819 VALUES LESS THAN (819),
                       |PARTITION p820 VALUES LESS THAN (820),
                       |PARTITION p821 VALUES LESS THAN (821),
                       |PARTITION p822 VALUES LESS THAN (822),
                       |PARTITION p823 VALUES LESS THAN (823),
                       |PARTITION p824 VALUES LESS THAN (824),
                       |PARTITION p825 VALUES LESS THAN (825),
                       |PARTITION p826 VALUES LESS THAN (826),
                       |PARTITION p827 VALUES LESS THAN (827),
                       |PARTITION p828 VALUES LESS THAN (828),
                       |PARTITION p829 VALUES LESS THAN (829),
                       |PARTITION p830 VALUES LESS THAN (830),
                       |PARTITION p831 VALUES LESS THAN (831),
                       |PARTITION p832 VALUES LESS THAN (832),
                       |PARTITION p833 VALUES LESS THAN (833),
                       |PARTITION p834 VALUES LESS THAN (834),
                       |PARTITION p835 VALUES LESS THAN (835),
                       |PARTITION p836 VALUES LESS THAN (836),
                       |PARTITION p837 VALUES LESS THAN (837),
                       |PARTITION p838 VALUES LESS THAN (838),
                       |PARTITION p839 VALUES LESS THAN (839),
                       |PARTITION p840 VALUES LESS THAN (840),
                       |PARTITION p841 VALUES LESS THAN (841),
                       |PARTITION p842 VALUES LESS THAN (842),
                       |PARTITION p843 VALUES LESS THAN (843),
                       |PARTITION p844 VALUES LESS THAN (844),
                       |PARTITION p845 VALUES LESS THAN (845),
                       |PARTITION p846 VALUES LESS THAN (846),
                       |PARTITION p847 VALUES LESS THAN (847),
                       |PARTITION p848 VALUES LESS THAN (848),
                       |PARTITION p849 VALUES LESS THAN (849),
                       |PARTITION p850 VALUES LESS THAN (850),
                       |PARTITION p851 VALUES LESS THAN (851),
                       |PARTITION p852 VALUES LESS THAN (852),
                       |PARTITION p853 VALUES LESS THAN (853),
                       |PARTITION p854 VALUES LESS THAN (854),
                       |PARTITION p855 VALUES LESS THAN (855),
                       |PARTITION p856 VALUES LESS THAN (856),
                       |PARTITION p857 VALUES LESS THAN (857),
                       |PARTITION p858 VALUES LESS THAN (858),
                       |PARTITION p859 VALUES LESS THAN (859),
                       |PARTITION p860 VALUES LESS THAN (860),
                       |PARTITION p861 VALUES LESS THAN (861),
                       |PARTITION p862 VALUES LESS THAN (862),
                       |PARTITION p863 VALUES LESS THAN (863),
                       |PARTITION p864 VALUES LESS THAN (864),
                       |PARTITION p865 VALUES LESS THAN (865),
                       |PARTITION p866 VALUES LESS THAN (866),
                       |PARTITION p867 VALUES LESS THAN (867),
                       |PARTITION p868 VALUES LESS THAN (868),
                       |PARTITION p869 VALUES LESS THAN (869),
                       |PARTITION p870 VALUES LESS THAN (870),
                       |PARTITION p871 VALUES LESS THAN (871),
                       |PARTITION p872 VALUES LESS THAN (872),
                       |PARTITION p873 VALUES LESS THAN (873),
                       |PARTITION p874 VALUES LESS THAN (874),
                       |PARTITION p875 VALUES LESS THAN (875),
                       |PARTITION p876 VALUES LESS THAN (876),
                       |PARTITION p877 VALUES LESS THAN (877),
                       |PARTITION p878 VALUES LESS THAN (878),
                       |PARTITION p879 VALUES LESS THAN (879),
                       |PARTITION p880 VALUES LESS THAN (880),
                       |PARTITION p881 VALUES LESS THAN (881),
                       |PARTITION p882 VALUES LESS THAN (882),
                       |PARTITION p883 VALUES LESS THAN (883),
                       |PARTITION p884 VALUES LESS THAN (884),
                       |PARTITION p885 VALUES LESS THAN (885),
                       |PARTITION p886 VALUES LESS THAN (886),
                       |PARTITION p887 VALUES LESS THAN (887),
                       |PARTITION p888 VALUES LESS THAN (888),
                       |PARTITION p889 VALUES LESS THAN (889),
                       |PARTITION p890 VALUES LESS THAN (890),
                       |PARTITION p891 VALUES LESS THAN (891),
                       |PARTITION p892 VALUES LESS THAN (892),
                       |PARTITION p893 VALUES LESS THAN (893),
                       |PARTITION p894 VALUES LESS THAN (894),
                       |PARTITION p895 VALUES LESS THAN (895),
                       |PARTITION p896 VALUES LESS THAN (896),
                       |PARTITION p897 VALUES LESS THAN (897),
                       |PARTITION p898 VALUES LESS THAN (898),
                       |PARTITION p899 VALUES LESS THAN (899),
                       |PARTITION p900 VALUES LESS THAN (900),
                       |PARTITION p901 VALUES LESS THAN (901),
                       |PARTITION p902 VALUES LESS THAN (902),
                       |PARTITION p903 VALUES LESS THAN (903),
                       |PARTITION p904 VALUES LESS THAN (904),
                       |PARTITION p905 VALUES LESS THAN (905),
                       |PARTITION p906 VALUES LESS THAN (906),
                       |PARTITION p907 VALUES LESS THAN (907),
                       |PARTITION p908 VALUES LESS THAN (908),
                       |PARTITION p909 VALUES LESS THAN (909),
                       |PARTITION p910 VALUES LESS THAN (910),
                       |PARTITION p911 VALUES LESS THAN (911),
                       |PARTITION p912 VALUES LESS THAN (912),
                       |PARTITION p913 VALUES LESS THAN (913),
                       |PARTITION p914 VALUES LESS THAN (914),
                       |PARTITION p915 VALUES LESS THAN (915),
                       |PARTITION p916 VALUES LESS THAN (916),
                       |PARTITION p917 VALUES LESS THAN (917),
                       |PARTITION p918 VALUES LESS THAN (918),
                       |PARTITION p919 VALUES LESS THAN (919),
                       |PARTITION p920 VALUES LESS THAN (920),
                       |PARTITION p921 VALUES LESS THAN (921),
                       |PARTITION p922 VALUES LESS THAN (922),
                       |PARTITION p923 VALUES LESS THAN (923),
                       |PARTITION p924 VALUES LESS THAN (924),
                       |PARTITION p925 VALUES LESS THAN (925),
                       |PARTITION p926 VALUES LESS THAN (926),
                       |PARTITION p927 VALUES LESS THAN (927),
                       |PARTITION p928 VALUES LESS THAN (928),
                       |PARTITION p929 VALUES LESS THAN (929),
                       |PARTITION p930 VALUES LESS THAN (930),
                       |PARTITION p931 VALUES LESS THAN (931),
                       |PARTITION p932 VALUES LESS THAN (932),
                       |PARTITION p933 VALUES LESS THAN (933),
                       |PARTITION p934 VALUES LESS THAN (934),
                       |PARTITION p935 VALUES LESS THAN (935),
                       |PARTITION p936 VALUES LESS THAN (936),
                       |PARTITION p937 VALUES LESS THAN (937),
                       |PARTITION p938 VALUES LESS THAN (938),
                       |PARTITION p939 VALUES LESS THAN (939),
                       |PARTITION p940 VALUES LESS THAN (940),
                       |PARTITION p941 VALUES LESS THAN (941),
                       |PARTITION p942 VALUES LESS THAN (942),
                       |PARTITION p943 VALUES LESS THAN (943),
                       |PARTITION p944 VALUES LESS THAN (944),
                       |PARTITION p945 VALUES LESS THAN (945),
                       |PARTITION p946 VALUES LESS THAN (946),
                       |PARTITION p947 VALUES LESS THAN (947),
                       |PARTITION p948 VALUES LESS THAN (948),
                       |PARTITION p949 VALUES LESS THAN (949),
                       |PARTITION p950 VALUES LESS THAN (950),
                       |PARTITION p951 VALUES LESS THAN (951),
                       |PARTITION p952 VALUES LESS THAN (952),
                       |PARTITION p953 VALUES LESS THAN (953),
                       |PARTITION p954 VALUES LESS THAN (954),
                       |PARTITION p955 VALUES LESS THAN (955),
                       |PARTITION p956 VALUES LESS THAN (956),
                       |PARTITION p957 VALUES LESS THAN (957),
                       |PARTITION p958 VALUES LESS THAN (958),
                       |PARTITION p959 VALUES LESS THAN (959),
                       |PARTITION p960 VALUES LESS THAN (960),
                       |PARTITION p961 VALUES LESS THAN (961),
                       |PARTITION p962 VALUES LESS THAN (962),
                       |PARTITION p963 VALUES LESS THAN (963),
                       |PARTITION p964 VALUES LESS THAN (964),
                       |PARTITION p965 VALUES LESS THAN (965),
                       |PARTITION p966 VALUES LESS THAN (966),
                       |PARTITION p967 VALUES LESS THAN (967),
                       |PARTITION p968 VALUES LESS THAN (968),
                       |PARTITION p969 VALUES LESS THAN (969),
                       |PARTITION p970 VALUES LESS THAN (970),
                       |PARTITION p971 VALUES LESS THAN (971),
                       |PARTITION p972 VALUES LESS THAN (972),
                       |PARTITION p973 VALUES LESS THAN (973),
                       |PARTITION p974 VALUES LESS THAN (974),
                       |PARTITION p975 VALUES LESS THAN (975),
                       |PARTITION p976 VALUES LESS THAN (976),
                       |PARTITION p977 VALUES LESS THAN (977),
                       |PARTITION p978 VALUES LESS THAN (978),
                       |PARTITION p979 VALUES LESS THAN (979),
                       |PARTITION p980 VALUES LESS THAN (980),
                       |PARTITION p981 VALUES LESS THAN (981),
                       |PARTITION p982 VALUES LESS THAN (982),
                       |PARTITION p983 VALUES LESS THAN (983),
                       |PARTITION p984 VALUES LESS THAN (984),
                       |PARTITION p985 VALUES LESS THAN (985),
                       |PARTITION p986 VALUES LESS THAN (986),
                       |PARTITION p987 VALUES LESS THAN (987),
                       |PARTITION p988 VALUES LESS THAN (988),
                       |PARTITION p989 VALUES LESS THAN (989),
                       |PARTITION p990 VALUES LESS THAN (990),
                       |PARTITION p991 VALUES LESS THAN (991),
                       |PARTITION p992 VALUES LESS THAN (992),
                       |PARTITION p993 VALUES LESS THAN (993),
                       |PARTITION p994 VALUES LESS THAN (994),
                       |PARTITION p995 VALUES LESS THAN (995),
                       |PARTITION p996 VALUES LESS THAN (996),
                       |PARTITION p997 VALUES LESS THAN (997),
                       |PARTITION p998 VALUES LESS THAN (998),
                       |PARTITION p999 VALUES LESS THAN (999),
                       |PARTITION p1000 VALUES LESS THAN (1000),
                       |PARTITION p1001 VALUES LESS THAN (1001),
                       |PARTITION p1002 VALUES LESS THAN (1002),
                       |PARTITION p1003 VALUES LESS THAN (1003),
                       |PARTITION p1004 VALUES LESS THAN (1004),
                       |PARTITION p1005 VALUES LESS THAN (1005),
                       |PARTITION p1006 VALUES LESS THAN (1006),
                       |PARTITION p1007 VALUES LESS THAN (1007),
                       |PARTITION p1008 VALUES LESS THAN (1008),
                       |PARTITION p1009 VALUES LESS THAN (1009),
                       |PARTITION p1010 VALUES LESS THAN (1010),
                       |PARTITION p1011 VALUES LESS THAN (1011),
                       |PARTITION p1012 VALUES LESS THAN (1012),
                       |PARTITION p1013 VALUES LESS THAN (1013),
                       |PARTITION p1014 VALUES LESS THAN (1014),
                       |PARTITION p1015 VALUES LESS THAN (1015),
                       |PARTITION p1016 VALUES LESS THAN (1016),
                       |PARTITION p1017 VALUES LESS THAN (1017),
                       |PARTITION p1018 VALUES LESS THAN (1018),
                       |PARTITION p1019 VALUES LESS THAN (1019),
                       |PARTITION p1020 VALUES LESS THAN (1020),
                       |PARTITION p1021 VALUES LESS THAN (1021),
                       |PARTITION p1022 VALUES LESS THAN (1022),
                       |PARTITION p1023 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)

    tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
    refreshConnections()

    judge("select * from pt")
  }

  test("test read from range partition and partition function (mod) is not supported by tispark") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (mod(year(purchased), 4)) (
                       |  PARTITION p0 VALUES LESS THAN (1),
                       |  PARTITION p1 VALUES LESS THAN (2),
                       |  PARTITION p2 VALUES LESS THAN (3),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)

    tidbStmt.execute("insert into `pt` values(1, 'name', '1995-10-10')")
    refreshConnections()

    judge("select * from pt")
    judge("select * from pt where name = 'name'")
    judge("select * from pt where name != 'name'")
    judge("select * from pt where purchased = date'1995-10-10'")
    judge("select * from pt where purchased != date'1995-10-10'")
  }

  test("reading from hash partition") {
    enablePartitionForTiDB()
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute(
      """create table t (id int) partition by hash(id) PARTITIONS 4
        |""".stripMargin
    )
    tidbStmt.execute("insert into `t` values(5)")
    tidbStmt.execute("insert into `t` values(15)")
    tidbStmt.execute("insert into `t` values(25)")
    tidbStmt.execute("insert into `t` values(35)")
    refreshConnections()

    judge("select * from t")
    judge("select * from t where id < 10")
  }

  test("constant folding does not apply case") {
    enablePartitionForTiDB()
    tidbStmt.execute(
      "create table t3 (c1 int) partition by range(c1) (partition p0 values less than maxvalue)"
    )
    tidbStmt.execute("insert into `t3` values(2)")
    tidbStmt.execute("insert into `t3` values(3)")
    refreshConnections()

    judge("select * from t3 where c1 > 2 + c1")
  }

  test("single maxvalue partition table case and part expr is not column") {
    enablePartitionForTiDB()
    tidbStmt.execute(
      "create table t2 (c1 int) partition by range(c1 + 1) (partition p0 values less than maxvalue)"
    )
    tidbStmt.execute("insert into `t2` values(2)")
    tidbStmt.execute("insert into `t2` values(3)")
    refreshConnections()

    judge("select * from t2 where c1 = 2")
  }

  // FIXME: https://github.com/pingcap/tispark/issues/701
  test("index scan on partition table") {
    enablePartitionForTiDB()
    tidbStmt.execute("drop table if exists p_t")
    tidbStmt.execute(
      "CREATE TABLE `p_t` (`id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin PARTITION BY RANGE ( id ) (   PARTITION p0 VALUES LESS THAN (2),   PARTITION p1 VALUES LESS THAN (4),   PARTITION p2 VALUES LESS THAN (6) );"
    )
    tidbStmt.execute("insert into `p_t` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `p_t` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `p_t` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `p_t` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `p_t` values(5, '1999-10-10')")
    refreshConnections()
    explainAndRunTest("select * from p_t where y = date'1996-10-10'", skipJDBC = true)
  }

  test("simple partition pruning test") {
    enablePartitionForTiDB()
    tidbStmt.execute(
      "CREATE TABLE `pt2` (   `id` int(11) DEFAULT NULL, `y` date DEFAULT NULL,   index `idx_y`(`y`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin " +
        "PARTITION BY RANGE ( id ) (   " +
        "PARTITION p0 VALUES LESS THAN (2),   " +
        "PARTITION p1 VALUES LESS THAN (4),   " +
        "PARTITION p2 VALUES LESS THAN (6) );"
    )
    tidbStmt.execute("insert into `pt2` values(1, '1995-10-10')")
    tidbStmt.execute("insert into `pt2` values(2, '1996-10-10')")
    tidbStmt.execute("insert into `pt2` values(3, '1997-10-10')")
    tidbStmt.execute("insert into `pt2` values(4, '1998-10-10')")
    tidbStmt.execute("insert into `pt2` values(5, '1999-10-10')")
    refreshConnections()
    judge("select * from pt2 where y = date'1996-10-10' or id < 2 and id > 6")
  }

  private def extractDAGReq(df: DataFrame): TiDAGRequest = {
    enablePartitionForTiDB()
    val executedPlan = df.queryExecution.executedPlan
    val copRDD = executedPlan.find(e => e.isInstanceOf[CoprocessorRDD])
    val regionTaskExec = executedPlan.find(e => e.isInstanceOf[RegionTaskExec])
    if (copRDD.isDefined) {
      copRDD.get
        .asInstanceOf[CoprocessorRDD]
        .tiRdds(0)
        .dagRequest
    } else {
      regionTaskExec.get
        .asInstanceOf[RegionTaskExec]
        .dagRequest
    }
  }

  test("part pruning on unix_timestamp") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt4`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt4` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` timestamp DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (unix_timestamp(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (unix_timestamp('1995-10-10')),
                       |  PARTITION p1 VALUES LESS THAN (unix_timestamp('2000-10-10')),
                       |  PARTITION p2 VALUES LESS THAN (unix_timestamp('2005-10-10'))
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      extractDAGReq(
        spark
          .sql("select * from pt4 where purchased = date'1994-10-10'")
      ).getPrunedParts
        .size() == 3
    )
  }

  test("part pruning on year function") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `pt3`")
    tidbStmt.execute("""
                       |CREATE TABLE `pt3` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (year(purchased)) (
                       |  PARTITION p0 VALUES LESS THAN (1995),
                       |  PARTITION p1 VALUES LESS THAN (2000),
                       |  PARTITION p2 VALUES LESS THAN (2005),
                       |  PARTITION p3 VALUES LESS THAN (MAXVALUE)
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from pt3 where purchased = date'1994-10-10'")
      ).getPrunedParts
        .get(0)
        .getName == "p0"
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql(
            "select * from pt3 where purchased > date'1996-10-10' and purchased < date'2000-10-10'"
          )
      ).getPrunedParts
        .get(0)
        .getName == "p1"
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains two parts which are p0 and p1.
            .sql("select * from pt3 where purchased < date'2000-10-10'")
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains one part which is p1.
            .sql(
              "select * from pt3 where purchased < date'2005-10-10' and purchased > date'2000-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p2"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // or with an unrelated column. All parts should be accessed.
            .sql(
              "select * from pt3 where id < 4 or purchased < date'1995-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 4
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          // for complicated expression, we do not support for now.
          // this will be improved later.
          spark
            .sql(
              "select * from pt3 where year(purchased) < 1995"
            )
        ).getPrunedParts
        pDef.size() == 4
      }
    )
  }

  test("adding part pruning test when index is on partitioned column") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL,
                       |  index `idx_id`(`id`)
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (id) (
                       |  PARTITION p0 VALUES LESS THAN (2),
                       |  PARTITION p1 VALUES LESS THAN (4),
                       |  PARTITION p2 VALUES LESS THAN (6)
                       |)
                     """.stripMargin)
    refreshConnections()
    assert(
      extractDAGReq(
        spark.sql("select * from p_t")
      ).getPrunedParts
        .size() == 3
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p.
          .sql("select * from p_t where id = 3")
      ).getPrunedParts
        .get(0)
        .getName == "p1"
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 4")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains two parts which are p0 and p1.
            .sql("select * from p_t where id < 4")
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains one part which is p1.
            .sql(
              "select * from p_t where id < 4 and id > 2"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // or with an unrelated column. All parts should be accessed.
            .sql(
              "select * from p_t where id < 4 and id > 2 or purchased = date'1995-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 3
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // and with an unrelated column. only p1 should be accessed.
            .sql(
              "select * from p_t where id < 4 and id > 2 and purchased = date'1995-10-10'"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )
  }

  test("adding part pruning test") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE (id) (
                       |  PARTITION p0 VALUES LESS THAN (2),
                       |  PARTITION p1 VALUES LESS THAN (4),
                       |  PARTITION p2 VALUES LESS THAN (6)
                       |)
                     """.stripMargin)
    refreshConnections()

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // or with a unrelated column, all partition should be accessed.
            .sql(
              "select * from p_t where id > 4 or id < 6 or purchased > date'1998-10-09'"
            )
        ).getPrunedParts
        pDef.size() == 3
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
            .sql(
              "select * from p_t where id > 4 and id < 6 and purchased > date'1998-10-09'"
            )
        ).getPrunedParts
        pDef.size() == 1
      }
    )

    assert(
      extractDAGReq(
        spark.sql("select * from p_t")
      ).getPrunedParts
        .size() == 3
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id = 5")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      extractDAGReq(
        spark
        // expected part info only contains one part which is p2.
          .sql("select * from p_t where id > 5")
      ).getPrunedParts
        .get(0)
        .getName == "p2"
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains two parts which are p0 and p1.
            .sql("select * from p_t where id < 4")
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contains one part which is p1.
            .sql(
              "select * from p_t where id < 4 and id > 2"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected parts info only contain two parts which is p0 and p2.
            .sql(
              "select * from p_t where id < 2 or id > 4"
            )
        ).getPrunedParts
        pDef.size() == 2 && pDef.get(0).getName == "p0" && pDef.get(1).getName == "p2"
      }
    )

    assert(
      {
        val pDef = extractDAGReq(
          spark
          // expected part info only contain one part which is p1.
            .sql(
              "select * from p_t where id > 2 and id < 4"
            )
        ).getPrunedParts
        pDef.size() == 1 && pDef.get(0).getName == "p1"
      }
    )
  }

  test("partition read(w/o pruning)") {
    enablePartitionForTiDB()
    tidbStmt.execute("DROP TABLE IF EXISTS `p_t`")
    tidbStmt.execute("""
                       |CREATE TABLE `p_t` (
                       |  `id` int(11) DEFAULT NULL,
                       |  `name` varchar(50) DEFAULT NULL,
                       |  `purchased` date DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
                       |PARTITION BY RANGE ( `id` ) (
                       |  PARTITION p0 VALUES LESS THAN (1990),
                       |  PARTITION p1 VALUES LESS THAN (1995),
                       |  PARTITION p2 VALUES LESS THAN (2000),
                       |  PARTITION p3 VALUES LESS THAN (2005)
                       |)
                     """.stripMargin)
    tidbStmt.execute("insert into p_t values (1, \"dede1\", \"1989-01-01\")")
    tidbStmt.execute("insert into p_t values (2, \"dede2\", \"1991-01-01\")")
    tidbStmt.execute("insert into p_t values (3, \"dede3\", \"1996-01-01\")")
    tidbStmt.execute("insert into p_t values (4, \"dede4\", \"1998-01-01\")")
    tidbStmt.execute("insert into p_t values (5, \"dede5\", \"2001-01-01\")")
    tidbStmt.execute("insert into p_t values (6, \"dede6\", \"2006-01-01\")")
    tidbStmt.execute("insert into p_t values (7, \"dede7\", \"2007-01-01\")")
    tidbStmt.execute("insert into p_t values (8, \"dede8\", \"2008-01-01\")")
    refreshConnections()
    assert(spark.sql("select * from p_t").count() == 8)
    judge("select count(*) from p_t where id = 1", checkLimit = false)
    judge("select id from p_t group by id", checkLimit = false)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists pt")
      tidbStmt.execute("drop table if exists pt2")
      tidbStmt.execute("drop table if exists p_t")
      tidbStmt.execute("drop table if exists pt3")
      tidbStmt.execute("drop table if exists pt4")
      tidbStmt.execute("drop table if exists t2")
      tidbStmt.execute("drop table if exists t3")
    } finally {
      super.afterAll()
    }
}
