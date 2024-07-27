drop table if exists product_image;
drop table if exists review_image;
drop table if exists user;
drop table if exists review;
drop table if exists product;
create table review (
    id bigint AUTO_INCREMENT not null primary key,
    rating double,
    title text,
    text text,
    asin char(10),
    parent_asin char(20),
    user_id char(36),
    timestamp bigint,
    helpful_vote int,
    index (asin, user_id)
);
create table product (
    id bigint AUTO_INCREMENT not null primary key,
    main_category text,
    title text,
    average_rating double,
    features text default null,
    description text default null,
    price double,
    store text,
    details json default null,
    parent_asin char(10),
    index (parent_asin)
);
create table review_image (
    review_id bigint,
    url text,
    info varchar(255),
    foreign key (review_id) references review(id),
    index (review_id)
);
create table product_image (
    product_id bigint,
    url text,
    info varchar(255),
    foreign key (product_id) references product(id),
    index (product_id)
);
create table user (
    user_id char(36),
    review_id bigint,
    foreign key (review_id) references review(id)
);
drop table if exists bad_image;
create table bad_image (
    url text
);